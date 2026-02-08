# DML Remote CAS + Refs Plan

This document defines the concrete storage layout and operational steps for DML remote storage using:

- immutable content-addressed objects (CAS)
- immutable-but-deletable refs for discoverability
- commit manifests (sidecars) to enable push/pull of only missing objects

---

## 1. Definitions

### 1.1 Object ID (OID)

- All IDs are `sha256` of canonical encoded bytes.
- OIDs are represented as lowercase hex strings (no `sha256:` prefix).

### 1.2 CAS

- CAS stores raw, immutable objects addressed by OID.
- CAS objects include commits, commit manifests, and any objects referenced by commits.
- CAS is the object store used by push/pull and repo GC.

### 1.3 Refs

- Refs are immutable pointers to **manifests**.
- Refs are deletable.
- Refs are the only GC roots.

Ref types:

- `tags/` – user-defined stable names (e.g. versions)
- `commits/` – convenience handles
- `cache/` – temporary system-managed refs (pruned before GC)

### 1.4 CAS Store API

A CAS store is a minimal interface for content-addressed storage.

Required operations:

- `put(bytes) -> oid`
- `get(oid) -> bytes`
- `has(oid) -> bool`

Batch operations are allowed but not required.

### 1.5 `S3CasStore`

`S3CasStore` is a CAS store implementation backed by S3.

Responsibilities:

- map OID -> S3 key using the remote CAS layout
- GET/PUT objects to S3
- optionally support efficient existence checks (e.g. HEAD) for push

---

## 2. Object Types

### 2.1 Commit (CAS object)

A semantic commit object.

- Stored in CAS under its OID.
- Contains parents/root pointers/metadata.
- Does **not** contain closure lists.

(Commit encoding is implementation-defined; it must be canonical.)

### 2.2 Manifest (CAS object)

A manifest is a CAS object that describes the full materialization closure for a root object.

The `root-ns` field identifies the namespace of the root object (e.g. `commit`).
The `root-id` field identifies the object ID within that namespace.

**Invariants**

- Manifest MUST include the root object identifier (`root-ns`, `root-id`).
- Closure MUST be complete: any object required to materialize the root locally MUST be listed.
- Manifest bytes MUST be canonical.
- Closure lists MUST be de-duplicated.

**Typed closure**

Because DML stores many object kinds, the manifest closure is recorded as a map:

- keys: object kinds (stable strings defined by the implementation)
- values: list of OIDs of that kind

Operationally (pull/GC), the closure is treated as the **union** of all OIDs across all kinds. Logic MUST NOT depend on kind labels for correctness.

Manifests are generic so that all database dump/load operations can use the same mechanism, making materialization explicit, uniform, and robust across object kinds.

**JSON example (schema v0)**:

```json
{
  "kind": "manifest",
  "schema": 0,
  "root-ns": "commit",
  "root-id": "1111111111111111111111111111111111111111111111111111111111111111",
  "closure": {
    "commit": [
      "1111111111111111111111111111111111111111111111111111111111111111"
    ],
    "tree": [
      "2222222222222222222222222222222222222222222222222222222222222222"
    ],
    "blob": ["3333333333333333333333333333333333333333333333333333333333333333"]
  }
}
```

### 2.3 Ref (remote object under `refs/`)

A small immutable record mapping a name to a manifest OID.

**JSON example**:

```
{
  "kind": "ref",
  "schema": 0,
  "target": "$1",
  "created_at": 1760000000,
  "meta": {
    "name": "foo",
    "version": "bar"
  }
}

```

**Cache ref JSON example** (includes pruning metadata):

```
{
  "kind": "ref",
  "schema": 0,
  "target": "$1",
  "created_at": 1760000000,
  "cache": {
    "expires_at": 1762592000
  },
  "meta": {
    "cache_key": "<cache-key>"
  }
}

```

---

## 3. Remote Layout (S3)

Remote root: `s3://<bucket>/<prefix>/`

```
<prefix>/
  dml.json

  refs/
    tags/
      <name>/<version>.json
    commits/
      <id>.json
    cache/
      <cache_key>/<id>.json

  cas/
    sha256/
      <aa>/<bb>/<fullhash>

```

### 3.1 `dml.json`

Remote descriptor.

**JSON example**:

```
{
  "schema": 0,
  "hash": "sha256",
  "layout": "cas+refs",
  "refs_prefix": "refs",
  "cas_prefix": "cas/sha256"
}

```

### 3.2 CAS sharding

`<aa>` = first two hex chars of hash, `<bb>` = next two.
Example:

- OID `abcd...` stored at `cas/sha256/ab/cd/abcd...`

---

## 4. Local Storage (LMDB)

Local storage is LMDB-backed (no flat files required).

The remote system uses the local LMDB as the authoritative local CAS and ref store.

### 4.1 Access pattern

All local reads/writes happen via a DB handle available as `self._db`:

- `with self._db.tx(readonly=...) as txn:`
- `txn.get(Ref("type:key"))`

Where `Ref` is an internal DB reference type.

### 4.2 Local keying

- The `key` portion is the **sha256** of the **messagepack-encoded** object bytes.
- The `type` portion selects the logical namespace.

Required namespaces:

- object namespaces are the semantic ones (e.g. `commit`, `tree`, `blob`, ...). Keys are of the form `<namespace>:<id>`.
- heads are stored as: `head:<remote-name>/<ref_path>` and point to a loaded root object (typically a commit).

Local object IO:

The remote protocol uses **only dict-based dump/load**.

- `txn.dump_dict(root_ref: Ref) -> dict` returns a **local-manifest** structure that inlines per-object dumps as strings (base64 of raw bytes (specifically what Python `base64.b64encode` returns)) keyed by `{ns: {id: dump_str}}`.
- `txn.load_dict(full_manifest: dict) -> None` loads a local-manifest back into the DB.

Notes:

- local-manifest objects are local-only and MUST NOT be uploaded to remote CAS.

- Base64 encoding is whatever Python `base64.b64encode` returns.

- local-manifest closure maps are NOT required to be sorted.

- remote `manifest` closure lists MUST be sorted (canonical encoding / stable OIDs).

- `local-manifest` objects are **local-only** and MUST NOT be uploaded to remote CAS.

- Remote CAS stores only raw object bytes and slim `manifest` objects (OID lists only).

### 4.3 Local invariant

For every local head, the referenced root object and every object in its closure exist locally.

---

## 5. Push Plan

### 5.1 Preconditions

- The local DB contains the root object referenced by the input ref.
- The client can compute the full closure (as `<namespace> -> [id...]`).

### 5.2 Steps

1. Call `txn.dump_dict(root_ref)` to produce a local **local-manifest**:
   `{ kind: "local-manifest", root-ns, root-id, closure: { ns: { id: dump_str } } }`.
2. For each `(ns, {id: dump_str})` entry in the local-manifest closure:
   - base64-decode `dump_str` to raw bytes
   - verify `sha256(bytes) == id`
   - upload missing bytes to remote `cas/sha256/**` using `id` as the CAS key

3. Convert the local-manifest into a remote **manifest**:
   - replace each `{ns: {id: dump_str}}` with `{ns: [id...]}` (keys only)
   - set `kind = "manifest"`
   - sort each namespace's OID list (do not assume local-manifest order)

4. Construct the manifest bytes (schema v0) from `{root-ns, root-id, closure}`.
   - Validate `root-ns == "commit"` before push.

5. Compute `manifest_id = sha256(manifest_bytes)` and upload the manifest to remote CAS if missing.
6. Write the ref JSON under `refs/<type>/...` pointing to `manifest_id` (immutable: fail if it already exists).

**Remote writes on push**

- `cas/sha256/**` for uploaded objects
- one new ref file under `refs/.../*.json`

---

## 6. Pull Plan

### 6.1 Steps

1. Fetch the ref JSON from remote `refs/<ref_path>`.
2. Fetch the target manifest CAS object from remote by `target`.
3. Decode the manifest and read `root-ns`, `root-id`, and `closure`.
   - Validate `root-ns == "commit"` before materialization.

4. Construct a local **local-manifest** structure:
   - for each `(ns, ids)` in `closure`:
     - fetch missing CAS bytes by `id`
     - verify `sha256(bytes) == id`
     - base64-encode bytes as `dump_str`

5. Call `txn.load_dict(full_manifest)`.
6. Write a head pointer:
   `head:<remote-name>/<ref_path> -> commit:<root-id>`.

**Local writes on pull**

- loaded objects via `txn.load(...)`
- one head pointer: `head:<remote-name>/<ref_path>`

---

## 7. Cache Refs Policy

Cache refs are temporary system-managed refs.

- They prevent GC while present.
- They are pruned before GC based on `cache.expires_at` (and/or other policy).

---

## 8. Garbage Collection Plan

GC is mark-and-sweep from refs.

### 8.1 Steps

1. Delete stale cache refs under `refs/cache/**` (e.g. `cache.expires_at < now`).
2. Enumerate all remaining refs (`refs/tags/**`, `refs/commits/**`, `refs/cache/**`).
3. Build `live_oids` for CAS:
   - include every ref target manifest OID
   - for each manifest, include every OID in its `closure` (union across kinds)

4. Delete CAS objects whose OID is **not** in `live_oids`, subject to a safety window.

### 8.2 Safety window

Do not delete CAS objects newer than a configured minimum age (e.g. 24–72 hours).
This prevents races with in-flight pushes.

---

## 9. Required Invariants

- CAS objects are immutable.
- Refs are immutable but deletable.
- Refs always point to manifests.
- Manifests provide complete closure lists.
- GC roots are refs; reachability is defined only via manifests.

---

## 10. Implementation

All remote functionality is implemented in a single module:

- `remote.py` exposing a single public entrypoint: `RemoteOps`

Internally, `RemoteOps` implements S3 CAS/ref operations, manifest/ref encoding/decoding, and GC. No other public modules are required.

### 10.1 Notes on IDs

- The OID used for remote addressing is the sha256 of the canonical messagepack bytes.
- The local DB may store additional indexes, but remote CAS keys are always OIDs.
