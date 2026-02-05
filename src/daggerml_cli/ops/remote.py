"""Remote operations for CAS + refs backed by S3.

This module provides RemoteOps, a class that handles pushing and pulling
repository state to/from S3-backed remote storage.
"""

import base64
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

from daggerml_cli._db import Ref
from daggerml_cli.ops.base_ops import BaseOps
from daggerml_cli.types import DmlRepoError, Head

try:
    import boto3
except ImportError:
    boto3 = None


def _get_s3_client():
    if boto3 is None:
        raise ImportError("boto3 is required for RemoteOps but is not installed.")
    return boto3.client("s3")


class RemoteError(Exception):
    """Base exception for remote operations."""

    pass


class RefAlreadyExists(Exception):
    """Raised when attempting to create a ref that already exists."""

    pass


class InvalidOid(Exception):
    """Raised when an object ID is invalid."""

    pass


class InvalidManifest(Exception):
    """Raised when a manifest is invalid."""

    pass


class InvalidRef(Exception):
    """Raised when a ref is invalid."""

    pass


class MissingCasObject(Exception):
    """Raised when a CAS object is missing."""

    pass


class ShaMismatch(Exception):
    """Raised when SHA256 verification fails."""

    pass


@dataclass
class RemoteOps(BaseOps):
    """Remote operations for CAS + refs backed by S3.

    This class provides methods to push and pull repository state
    between local storage and remote S3-backed storage.
    """

    client: Any = field(default_factory=_get_s3_client)
    bucket: str = field(default_factory=lambda: os.environ.get("DML_S3_BUCKET", ""))
    prefix: str = field(default_factory=lambda: os.environ.get("DML_S3_PREFIX", ""))

    def __post_init__(self):
        self._ensure_remote_descriptor()
        super().__post_init__()

    def _ensure_remote_descriptor(self) -> None:
        """Ensure the remote prefix has a valid dml.json descriptor file.

        Creates the descriptor if missing, or overwrites invalid ones.
        The descriptor describes the remote layout and schema.
        """
        descriptor_key = f"{self.prefix}/dml.json" if self.prefix else "dml.json"
        expected_descriptor = {
            "schema": 0,
            "hash": "sha256",
            "layout": "cas+refs",
            "refs_prefix": "refs",
            "cas_prefix": "cas/sha256",
        }
        try:
            # Try to get existing descriptor
            response = self.client.get_object(Bucket=self.bucket, Key=descriptor_key)
            descriptor = json.loads(response["Body"].read().decode("utf-8"))
            # Check if it matches expected descriptor
            if descriptor != expected_descriptor:
                # Descriptor exists but is invalid, overwrite it
                descriptor_json = json.dumps(expected_descriptor, separators=(",", ":"), sort_keys=True)
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=descriptor_key,
                    Body=descriptor_json.encode("utf-8"),
                    ContentType="application/json",
                )
        except self.client.exceptions.NoSuchKey:
            # Descriptor doesn't exist, create it
            descriptor_json = json.dumps(expected_descriptor, separators=(",", ":"), sort_keys=True)
            self.client.put_object(
                Bucket=self.bucket,
                Key=descriptor_key,
                Body=descriptor_json.encode("utf-8"),
                ContentType="application/json",
            )

    def _cas_key(self, oid: str) -> str:
        """Generate CAS key for object ID with sharding.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)

        Returns
        -------
        str
            S3 key for the CAS object

        Raises
        ------
        InvalidOid
            If oid is not a valid 64-character lowercase hex string
        """
        if not re.match(r"^[0-9a-f]{64}$", oid):
            raise InvalidOid(f"Invalid OID: must be 64 lowercase hex characters, got {oid!r}")
        aa = oid[:2]
        bb = oid[2:4]
        return f"{self.prefix}/cas/sha256/{aa}/{bb}/{oid}"

    def _ref_key(self, ref_path: str) -> str:
        """Generate ref key for reference path.

        Parameters
        ----------
        ref_path : str
            Reference path

        Returns
        -------
        str
            S3 key for the ref

        Raises
        ------
        ValueError
            If ref_path contains path traversal sequences
        """
        if ref_path.startswith("/") or ".." in ref_path:
            raise ValueError(f"Invalid ref path: cannot start with '/' or contain '..', got {ref_path!r}")
        return f"{self.prefix}/refs/{ref_path}"

    def _remote_has_cas(self, oid: str) -> bool:
        """Check if CAS object exists in remote storage.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)

        Returns
        -------
        bool
            True if the object exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._cas_key(oid))
            return True
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                return False
            raise

    def _remote_get_cas(self, oid: str) -> bytes:
        """Get CAS object data from remote storage.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)

        Returns
        -------
        bytes
            The object data

        Raises
        ------
        MissingCasObject
            If the object does not exist
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._cas_key(oid))
            return response["Body"].read()
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise MissingCasObject(f"CAS object {oid} not found") from None
            raise

    def _remote_put_cas(self, oid: str, data: bytes) -> None:
        """Put CAS object data to remote storage.

        Parameters
        ----------
        oid : str
            Object ID (64-character lowercase hex string)
        data : bytes
            The object data to store
        """
        self.client.put_object(
            Bucket=self.bucket,
            Key=self._cas_key(oid),
            Body=data,
        )

    def _remote_get_ref(self, ref_path: str) -> bytes:
        """Get ref data from remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path

        Returns
        -------
        bytes
            The ref data

        Raises
        ------
        RemoteError
            If the ref does not exist
        """
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._ref_key(ref_path))
            return response["Body"].read()
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ("NoSuchKey", "404"):
                raise RemoteError(f"Ref {ref_path} not found") from None
            raise

    def _remote_put_ref(self, ref_path: str, data: bytes) -> None:
        """Put ref data to remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path
        data : bytes
            The ref data to store

        Raises
        ------
        RefAlreadyExists
            If the ref already exists
        """
        # Check if ref already exists
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._ref_key(ref_path))
            raise RefAlreadyExists(f"Ref {ref_path} already exists")
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code not in ("NoSuchKey", "404"):
                raise

        # Put the ref
        self.client.put_object(
            Bucket=self.bucket,
            Key=self._ref_key(ref_path),
            Body=data,
        )

    def _remote_delete_ref(self, ref_path: str) -> None:
        """Delete ref from remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path
        """
        self.client.delete_object(Bucket=self.bucket, Key=self._ref_key(ref_path))

    def _decode_ref(self, data: bytes) -> dict:
        """Decode and validate ref data from bytes.

        Parameters
        ----------
        data : bytes
            JSON-encoded ref data

        Returns
        -------
        dict
            Decoded and validated ref object

        Raises
        ------
        InvalidRef
            If the data is not a valid ref
        """
        o = json.loads(data)
        if o.get("kind") != "ref":
            raise InvalidRef("Invalid ref: kind must be 'ref'")
        if o.get("schema") != 0:
            raise InvalidRef("Invalid ref: schema must be 0")
        target = o.get("target")
        if not isinstance(target, str) or not re.match(r"^[0-9a-f]{64}$", target):
            raise InvalidRef("Invalid ref: target must be 64 lowercase hex characters")
        created_at = o.get("created_at")
        if not isinstance(created_at, int):
            raise InvalidRef("Invalid ref: created_at must be an integer")
        return o

    def _decode_manifest(self, data: bytes) -> dict:
        """Decode and validate manifest data from bytes.

        Parameters
        ----------
        data : bytes
            JSON-encoded manifest data

        Returns
        -------
        dict
            Decoded and validated manifest object

        Raises
        ------
        InvalidManifest
            If the data is not a valid manifest
        """
        o = json.loads(data)
        if o.get("kind") != "manifest":
            raise InvalidManifest("Invalid manifest: kind must be 'manifest'")
        if o.get("schema") != 0:
            raise InvalidManifest("Invalid manifest: schema must be 0")
        if "root-ns" not in o or "root-id" not in o:
            raise InvalidManifest("Invalid manifest: must have 'root-ns' and 'root-id'")
        closure = o.get("closure")
        if not isinstance(closure, dict):
            raise InvalidManifest("Invalid manifest: 'closure' must be a dict")
        for kind, ids in closure.items():
            if not isinstance(ids, list):
                raise InvalidManifest(f"Invalid manifest: closure['{kind}'] must be a list")
            if ids != sorted(ids):
                raise InvalidManifest(f"Invalid manifest: closure['{kind}'] must be sorted")
            if len(ids) != len(set(ids)):
                raise InvalidManifest(f"Invalid manifest: closure['{kind}'] must have no duplicates")
            for oid in ids:
                if not isinstance(oid, str) or not re.match(r"^[0-9a-f]{64}$", oid):
                    raise InvalidManifest(f"Invalid manifest: oid '{oid}' must be 64 lowercase hex characters")
        return o

    def _closure_union(self, closure: dict[str, list[str]]) -> set[str]:
        """Compute the union of all OIDs across all closure kinds.

        Parameters
        ----------
        closure : dict[str, list[str]]
            Closure mapping from kind to list of OIDs

        Returns
        -------
        set[str]
            Set of all unique OIDs across all kinds
        """
        union_oids = set()
        for oids in closure.values():
            union_oids.update(oids)
        return union_oids

    def _local_dump_dict(self, txn, root_ref) -> dict:
        """Dump local manifest from transaction and validate/normalize shape.

        Parameters
        ----------
        txn : TxnContext
            Transaction context
        root_ref : Ref
            Root reference to dump

        Returns
        -------
        dict
            Normalized local manifest dictionary

        Raises
        ------
        ValueError
            If manifest shape is invalid
        """
        manifest = txn.dump_dict(root_ref)

        # Validate kind
        if manifest.get("kind") != "local-manifest":
            raise ValueError("Invalid local manifest: kind must be 'local-manifest'")

        # Check for schema - tolerate missing but normalize to 0
        schema = manifest.get("schema", 0)
        if schema != 0:
            raise ValueError("Invalid local manifest: schema must be 0")

        # Ensure required fields are present
        if "root-ns" not in manifest or "root-id" not in manifest:
            raise ValueError("Invalid local manifest: must have 'root-ns' and 'root-id'")

        # Validate closure structure
        closure = manifest.get("closure", {})
        if not isinstance(closure, dict):
            raise ValueError("Invalid local manifest: 'closure' must be a dict")

        for ns, items in closure.items():
            if not isinstance(items, dict):
                raise ValueError(f"Invalid local manifest: closure['{ns}'] must be a dict")
            for id_, dump_str in items.items():
                if not isinstance(dump_str, str):
                    raise ValueError(f"Invalid local manifest: closure['{ns}']['{id_}'] must be a string")

        # Return normalized manifest (ensuring schema is set)
        normalized = manifest.copy()
        normalized["schema"] = 0
        return normalized

    def _local_load_dict(self, txn, local_manifest: dict) -> None:
        """Load local manifest into transaction after validation.

        Parameters
        ----------
        txn : TxnContext
            Transaction context
        local_manifest : dict
            Local manifest dictionary to load

        Raises
        ------
        ValueError
            If manifest shape is invalid
        """
        # Validate kind
        if local_manifest.get("kind") != "local-manifest":
            raise ValueError("Invalid local manifest: kind must be 'local-manifest'")

        # Load into transaction
        txn.load_dict(local_manifest)

    def _local_has(self, txn, ns: str, id: str) -> bool:
        """Check if a local object exists in the given namespace.

        Parameters
        ----------
        txn : TxnContext
            Transaction context
        ns : str
            Namespace
        id : str
            Object ID

        Returns
        -------
        bool
            True if object exists, False otherwise
        """
        try:
            txn.get(Ref(f"{ns}:{id}"))
            return True
        except DmlRepoError:
            return False

    def _build_remote_manifest(self, local_manifest: dict) -> tuple[dict, bytes]:
        """Build remote manifest dict and canonical bytes from local manifest.

        Parameters
        ----------
        local_manifest : dict
            Local manifest dictionary with closure as {ns: {id: dump_str}}

        Returns
        -------
        tuple[dict, bytes]
            Remote manifest dict and canonical JSON bytes

        Raises
        ------
        ValueError
            If root-ns is not "commit"
        """
        # Validate root namespace (push requirement)
        root_ns = local_manifest["root-ns"]
        if root_ns != "commit":
            raise ValueError(f"Cannot push non-commit root namespace: {root_ns!r}")

        root_id = local_manifest["root-id"]

        # Convert closure: {ns: {id: dump_str}} -> {ns: sorted([id...])}
        remote_closure = {}
        for ns, items in local_manifest["closure"].items():
            # Extract IDs, dedupe, and sort
            ids = list(set(items.keys()))
            ids.sort()
            remote_closure[ns] = ids

        # Build remote manifest dict
        manifest_dict = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": root_ns,
            "root-id": root_id,
            "closure": remote_closure,
        }

        # Produce canonical bytes
        manifest_bytes = json.dumps(manifest_dict, separators=(",", ":"), sort_keys=True).encode("utf-8")

        return manifest_dict, manifest_bytes

    def _push_upload_objects(self, local_manifest: dict) -> None:
        """Upload missing CAS objects from local manifest closure.

        Iterates through all objects in the local manifest's closure,
        verifies SHA256 integrity, and uploads to remote storage if missing.

        Parameters
        ----------
        local_manifest : dict
            Local manifest dictionary with closure containing base64-encoded objects

        Raises
        ------
        ValueError
            If any object's SHA256 hash doesn't match its ID
        """
        closure = local_manifest.get("closure", {})

        for _ns, items in closure.items():
            for id_, dump_str in items.items():
                # Decode base64 to raw bytes
                raw = base64.b64decode(dump_str)

                # Verify SHA256 matches the ID
                computed_hash = hashlib.sha256(raw).hexdigest()
                if computed_hash != id_:
                    raise ShaMismatch(f"SHA256 mismatch for object {id_}: expected {id_}, got {computed_hash}")

                # Upload only if missing
                if not self._remote_has_cas(id_):
                    self._remote_put_cas(id_, raw)

    def push(self, ref: Ref) -> str:
        """Push a repository reference to remote storage.

        Parameters
        ----------
        ref : Ref
            Reference to push (e.g., Ref("commit:<id>"))

        Returns
        -------
        str
            The ref path where the reference was published

        Raises
        ------
        ValueError
            If the reference is not a commit reference
        RefAlreadyExists
            If the ref already exists remotely
        """
        # Step 1: Dump local manifest from database
        with self._tx(readonly=True) as txn:
            lm = self._local_dump_dict(txn, ref)

        # Step 2: Upload missing CAS objects
        self._push_upload_objects(lm)

        # Step 3-4: Build remote manifest and compute manifest ID
        manifest_dict, manifest_bytes = self._build_remote_manifest(lm)
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()

        # Step 5: Upload manifest CAS object if missing
        if not self._remote_has_cas(manifest_id):
            self._remote_put_cas(manifest_id, manifest_bytes)

        # Step 6: Build ref JSON bytes
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": int(time.time()),
            "meta": {},  # Optional metadata
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Step 7: Compute ref_path from ref
        if ref.ns() == "commit":
            ref_path = f"commits/{ref.id()}.json"
        else:
            # For now, only support commit refs
            # TODO: Add support for tags when needed
            raise ValueError(f"Unsupported ref namespace: {ref.ns()}. Only 'commit' refs are supported.")

        # Step 8: Create the ref (must fail if exists)
        self._remote_put_ref(ref_path, ref_bytes)

        # Step 9: Return ref_path
        return ref_path

    def pull(self, ref_path: str) -> None:
        """Pull a repository reference from remote storage.

        Parameters
        ----------
        ref_path : str
            Reference path to pull (e.g., "commits/abc123.json")

        Raises
        ------
        ValueError
            If the manifest has a non-commit root namespace
        RemoteError
            If the ref or manifest cannot be found
        """
        # Step 1: Get ref bytes
        ref_bytes = self._remote_get_ref(ref_path)

        # Step 2: Decode ref
        ref_obj = self._decode_ref(ref_bytes)

        # Step 3: Get manifest bytes from CAS
        manifest_bytes = self._remote_get_cas(ref_obj["target"])

        # Step 4: Decode manifest
        manifest = self._decode_manifest(manifest_bytes)

        # Step 5: Validate root namespace is "commit"
        if manifest["root-ns"] != "commit":
            raise ValueError(f"Cannot pull non-commit root namespace: {manifest['root-ns']!r}")

        # Step 6-7: Fetch missing CAS objects and build local manifest
        local_closure = {}
        for ns, ids in manifest["closure"].items():
            for oid in ids:
                # Check if we already have this object locally
                with self._tx(readonly=True) as txn:
                    if self._local_has(txn, ns, oid):
                        continue  # Skip download

                # Fetch the object from remote
                raw_bytes = self._remote_get_cas(oid)

                # Verify SHA256
                computed_hash = hashlib.sha256(raw_bytes).hexdigest()
                if computed_hash != oid:
                    raise ShaMismatch(f"SHA256 mismatch for object {oid}: expected {oid}, got {computed_hash}")

                # Base64 encode for local manifest
                dump_str = base64.b64encode(raw_bytes).decode("ascii")

                # Add to local closure
                if ns not in local_closure:
                    local_closure[ns] = {}
                local_closure[ns][oid] = dump_str

        # Step 8: Build local manifest
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": manifest["root-ns"],
            "root-id": manifest["root-id"],
            "closure": local_closure,
        }

        # Step 9-10: Load local manifest and write head pointer in write transaction
        remote_name = f"s3://{self.bucket}"
        if self.prefix:
            remote_name = f"s3://{self.bucket}/{self.prefix}"

        with self._tx(readonly=False) as txn:
            self._local_load_dict(txn, local_manifest)
            self._local_put_head(txn, remote_name, ref_path, manifest["root-id"])

    def list(self, prefix: str) -> list[dict]:
        """List remote refs for a given prefix (tags, commits, cache).

        Parameters
        ----------
        prefix : str
            The prefix to list refs for (e.g., "commits", "tags", "cache")

        Returns
        -------
        list[dict]
            List of dictionaries containing decoded ref information including
            meta data and inferred ref_path
        """
        refs = []

        # List objects under refs/<prefix>/
        prefix_key = f"{self.prefix}/refs/{prefix}/" if self.prefix else f"refs/{prefix}/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix_key):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                # Only process .json files
                if not key.endswith(".json"):
                    continue

                # Extract ref_path relative to refs/<prefix>/
                if self.prefix:
                    ref_path = key[len(f"{self.prefix}/refs/") :]
                else:
                    ref_path = key[len("refs/") :]

                try:
                    # Get and decode the ref
                    ref_bytes = self._remote_get_ref(ref_path)
                    ref_obj = self._decode_ref(ref_bytes)

                    # Add inferred ref_path to the result
                    ref_obj["ref_path"] = ref_path
                    refs.append(ref_obj)

                except (RemoteError, ValueError, json.JSONDecodeError):
                    # Skip refs that can't be retrieved or decoded
                    continue

        return refs

    def prune(self) -> int:
        """Delete expired cache refs.

        Returns
        -------
        int
            Number of cache refs deleted
        """
        now = int(time.time())
        deleted_count = 0

        # List objects under refs/cache/
        cache_prefix = f"{self.prefix}/refs/cache/" if self.prefix else "refs/cache/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=cache_prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                # Only process .json files
                if not key.endswith(".json"):
                    continue

                # Extract ref_path relative to refs/
                if self.prefix:
                    ref_path = key[len(f"{self.prefix}/refs/") :]
                else:
                    ref_path = key[len("refs/") :]

                try:
                    # Get and decode the ref
                    ref_bytes = self._remote_get_ref(ref_path)
                    ref_obj = self._decode_ref(ref_bytes)

                    # Check if it's a cache ref with expires_at
                    cache_meta = ref_obj.get("meta", {}).get("cache")
                    if cache_meta and isinstance(cache_meta, dict):
                        expires_at = cache_meta.get("expires_at")
                        if isinstance(expires_at, int) and expires_at < now:
                            # Delete the expired cache ref
                            self._remote_delete_ref(ref_path)
                            deleted_count += 1

                except (RemoteError, ValueError, json.JSONDecodeError, KeyError):
                    # Skip refs that can't be retrieved, decoded, or don't have expected structure
                    continue

        return deleted_count

    def _gc_mark(self) -> set[str]:
        """Build the set of live OIDs by marking reachable objects from refs.

        Refs are the roots; manifests define reachability through their closure.

        Returns
        -------
        set[str]
            Set of all live OIDs (manifest targets + closure union)
        """
        live_oids = set()

        # Get all refs (tags, commits, cache) after prune
        # Note: prune is not called here as it's assumed to be called before GC
        all_refs = []
        all_refs.extend(self.list("tags"))
        all_refs.extend(self.list("commits"))
        all_refs.extend(self.list("cache"))

        # For each ref, add its target (manifest OID) to live_oids
        for ref_obj in all_refs:
            manifest_oid = ref_obj["target"]
            live_oids.add(manifest_oid)

            # Fetch and decode the manifest
            try:
                manifest_bytes = self._remote_get_cas(manifest_oid)
                manifest = self._decode_manifest(manifest_bytes)
            except (RemoteError, ValueError):
                # Skip malformed manifests/refs
                continue

            # Add all OIDs from the manifest closure
            closure_oids = self._closure_union(manifest["closure"])
            live_oids.update(closure_oids)

        return live_oids

    def _gc_sweep(self, live_oids: set[str], min_age_seconds: int) -> dict[str, int]:
        """Perform GC sweep phase: delete unreferenced CAS objects older than safety window.

        Parameters
        ----------
        live_oids : set[str]
            Set of live OIDs that should not be deleted
        min_age_seconds : int
            Minimum age in seconds for objects to be eligible for deletion

        Returns
        -------
        dict[str, int]
            Summary with counts: {"deleted": n, "kept_live": n, "kept_young": n}
        """
        deleted = 0
        kept_live = 0
        kept_young = 0

        # Current time for age calculation
        now = int(time.time())

        # List all CAS objects under cas/sha256/
        cas_prefix = f"{self.prefix}/cas/sha256/" if self.prefix else "cas/sha256/"

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=cas_prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                last_modified = obj["LastModified"]

                # Extract OID from key: {prefix}/cas/sha256/{aa}/{bb}/{oid}
                # The OID is the last component after the last '/'
                oid = key.split("/")[-1]

                # Validate that this looks like an OID (64 hex chars)
                if not re.match(r"^[0-9a-f]{64}$", oid):
                    # Skip malformed keys
                    continue

                if oid in live_oids:
                    # Keep live objects
                    kept_live += 1
                    continue

                # Check age
                age_seconds = now - int(last_modified.timestamp())
                if age_seconds < min_age_seconds:
                    # Keep young objects
                    kept_young += 1
                    continue

                # Delete old, unreferenced object
                try:
                    self.client.delete_object(Bucket=self.bucket, Key=key)
                    deleted += 1
                except Exception:
                    # Skip deletion errors (object might have been deleted by another process)
                    pass

        return {
            "deleted": deleted,
            "kept_live": kept_live,
            "kept_young": kept_young,
        }

    def gc(self, min_age_seconds: int = 24 * 3600) -> dict[str, int]:
        """Run garbage collection on the remote storage.

        This performs mark-and-sweep GC where refs are the roots.
        First prunes expired cache refs, then marks live objects,
        then sweeps unreferenced objects older than the safety window.

        Parameters
        ----------
        min_age_seconds : int, optional
            Minimum age in seconds for unreferenced objects to be deleted.
            Defaults to 24 hours.

        Returns
        -------
        dict[str, int]
            Summary with counts: {"deleted": n, "kept_live": n, "kept_young": n}
        """
        # First prune expired cache refs
        self.prune()

        # Mark phase: build set of live OIDs
        live_oids = self._gc_mark()

        # Sweep phase: delete unreferenced objects older than safety window
        return self._gc_sweep(live_oids, min_age_seconds)

    def _local_put_head(self, txn, remote_name: str, ref_path: str, commit_id: str) -> None:
        """Store head pointer for a remote ref path.

        Parameters
        ----------
        txn : TxnContext
            Transaction context
        remote_name : str
            Remote name (e.g., "s3://bucket/prefix")
        ref_path : str
            Reference path
        commit_id : str
            Commit ID to point to
        """
        txn.put(Head(commit=Ref(f"commit:{commit_id}")), to=Ref(f"head:{remote_name}/{ref_path}"))
