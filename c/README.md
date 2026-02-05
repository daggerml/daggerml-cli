# C Core and Bindings

This document lists the C implementation units and the symbols defined in each file.

### Recent pruning

- Removed `dml_db_reopen`, the msgpack convenience helpers (`dml_db_put_msgpack`, `dml_db_get_msgpack`), and the datum/recursive wrappers because the Python shim now uses only the namespace-aware insert/get APIs.
- Dropped `dml_db_txn_begin` since all callers normalize options directly through `dml_db_txn_begin_with_options`.
- Removed the hardcoded default-namespace string; the primary namespace is tracked by index rather than name.

## c/src/dml_core.c

Core LMDB-backed database implementation and transaction helpers.

### struct DmlDbHandle

API: `struct DmlDbHandle`
Holds LMDB environment state, namespace metadata, and write-transaction tracking fields.

### struct DmlDbTxn

API: `struct DmlDbTxn`
Wraps an LMDB transaction handle with readonly and owning handle metadata.

### DmlDumpEntry

API: `typedef struct DmlDumpEntry`
Internal entry for dump payloads: key + raw value bytes.

### DmlDumpList

API: `typedef struct DmlDumpList`
Resizable list of dump entries used when walking ref graphs.

### dml_db_reset_handle

API: `static void dml_db_reset_handle(DmlDbHandle *handle)`
Zeroes all handle fields.

### dml_db_free_namespaces

API: `static void dml_db_free_namespaces(DmlDbHandle *handle)`
Frees namespace strings, DBI arrays, and open flags.

### dml_db_reset_runtime

API: `static void dml_db_reset_runtime(DmlDbHandle *handle)`
Clears runtime-open state without freeing namespaces.

### dml_db_copy_namespaces

API: `static int dml_db_copy_namespaces(DmlDbHandle *handle, const char *const *namespaces, size_t namespace_count)`
Copies namespace strings into the handle and allocates DBI storage.

### dml_db_open_dbi

API: `static int dml_db_open_dbi(DmlDbHandle *handle, size_t index)`
Opens a namespace DBI in its own write transaction with mutex protection.

### dml_db_open_dbi_txn

API: `static int dml_db_open_dbi_txn(DmlDbHandle *handle, MDB_txn *txn, size_t index)`
Opens a namespace DBI using an existing transaction.

### dml_db_open_namespaces

API: `static int dml_db_open_namespaces(DmlDbHandle *handle, MDB_txn *txn, int create)`
Opens all namespaces in a single transaction.

### dml_db_lookup_namespace

API: `static int dml_db_lookup_namespace(DmlDbHandle *handle, const char *namespace_str, size_t namespace_len, MDB_dbi *out_dbi)`
Finds or opens a DBI by namespace string.

### dml_db_lookup_namespace_txn

API: `static int dml_db_lookup_namespace_txn(DmlDbHandle *handle, MDB_txn *txn, const char *namespace_str, size_t namespace_len, MDB_dbi *out_dbi)`
Namespace lookup using an existing transaction.

### dml_dump_list_free

API: `static void dml_dump_list_free(DmlDumpList *list)`
Frees dump list entries and resets counters.

### dml_dump_entry_compare

API: `static int dml_dump_entry_compare(const void *left, const void *right)`
Compares dump entries by key for sorting.

### dml_dump_list_find

API: `static int dml_dump_list_find(const DmlDumpList *list, const char *key, size_t key_len)`
Returns index of a key in the dump list.

### dml_dump_list_add

API: `static int dml_dump_list_add(DmlDumpList *list, const char *key, size_t key_len, void *value, size_t value_len)`
Appends a new key/value payload to the dump list.

### dml_dump_add_ref

API: `static int dml_dump_add_ref(DmlDbHandle *handle, DmlDumpList *list, const char *key, size_t key_len)`
Loads a referenced value and adds it to the dump list.

### dml_dump_visit_value

API: `static int dml_dump_visit_value(DmlDbHandle *handle, DmlDumpList *list, const DmlValue *value)`
Walks a value graph and queues referenced nodes for dumping.

### dml_db_init_handle_with_namespaces

API: `static int dml_db_init_handle_with_namespaces(DmlDbHandle *handle, const char *path, int create, const char *const *namespaces, size_t namespace_count)`
Initializes LMDB env and namespace metadata.

### dml_db_validate

API: `static int dml_db_validate(DmlDbHandle *handle)`
Validates open state and fork safety.

### dml_db_create

API: `int dml_db_create(const char *path, DmlDbHandle **out_handle)`
Creates a new env with the built-in namespace set.

### dml_db_create_with_namespaces

API: `int dml_db_create_with_namespaces(const char *path, const char *const *namespaces, size_t namespace_count, DmlDbHandle **out_handle)`
Creates a new env with custom namespaces.

### dml_db_open

API: `int dml_db_open(const char *path, DmlDbHandle **out_handle)`
Opens an existing env with the built-in namespace set.

### dml_db_open_with_namespaces

API: `int dml_db_open_with_namespaces(const char *path, const char *const *namespaces, size_t namespace_count, DmlDbHandle **out_handle)`
Opens an existing env with custom namespaces.

### dml_db_put

API: `int dml_db_put(DmlDbHandle *handle, const char *key, size_t key_len, const char *value, size_t value_len)`
Stores raw bytes under a full ref key.

### dml_db_get

API: `int dml_db_get(DmlDbHandle *handle, const char *key, size_t key_len, DmlDbValue *out_value)`
Loads raw bytes for a ref key.

### dml_db_insert

API: `int dml_db_insert(DmlDbHandle *handle, const char *namespace_str, size_t namespace_len, const char *key_override, size_t key_override_len, const char *value, size_t value_len, DmlValue **out_ref)`
Stores a value in a namespace and returns a `Ref` value.

### dml_db_txn_put

API: `static int dml_db_txn_put(DmlDbTxn *txn, const char *key, size_t key_len, const char *value, size_t value_len)`
Write helper for an active transaction.

### dml_db_txn_insert

API: `int dml_db_txn_insert(DmlDbTxn *txn, const char *namespace_str, size_t namespace_len, const char *key_override, size_t key_override_len, const char *value, size_t value_len, DmlValue **out_ref)`
Transaction-aware insert helper.

### dml_db_resize

API: `int dml_db_resize(DmlDbHandle *handle, size_t mapsize)`
Updates the LMDB map size.

### dml_db_mapsize

API: `int dml_db_mapsize(DmlDbHandle *handle, size_t *out_mapsize)`
Returns the current LMDB map size.

### dml_db_iter_keys

API: `int dml_db_iter_keys(DmlDbHandle *handle, const char *prefix, size_t prefix_len, const char *start_token, size_t start_len, DmlDbKeyPage *out_page)`
Lists keys with optional namespace/start token.

### dml_db_free_key_page

API: `void dml_db_free_key_page(DmlDbKeyPage *page)`
Frees key page allocations produced by `dml_db_iter_keys`.

### dml_db_txn_commit

API: `int dml_db_txn_commit(DmlDbTxn *txn)`
Commits a transaction and releases resources.

### dml_db_txn_abort

API: `void dml_db_txn_abort(DmlDbTxn *txn)`
Aborts a transaction and releases resources.

### dml_db_close

API: `void dml_db_close(DmlDbHandle *handle)`
Closes LMDB env and clears runtime state.

### dml_db_destroy

API: `void dml_db_destroy(DmlDbHandle *handle)`
Closes and frees the handle and namespace storage.

### dml_db_free_value

API: `void dml_db_free_value(void *data)`
Frees heap memory returned by DB reads/dumps.

## c/src/dml_hash.c

SHA-256 helpers for key generation.

### dml_hash_sha256_hex

API: `int dml_hash_sha256_hex(const void *data, size_t len, char out[65])`
Computes a hex SHA-256 digest into a 65-byte buffer.

### UB fix in sha256_transform

File: `c/third_party/sha256/sha256.c`, line 49
Fixed undefined behavior in word assembly by casting each BYTE to WORD before shifting.
Without this cast, integer promotion to signed `int` followed by left shift into sign bit is UB.

```c
m[i] = ((WORD)data[j] << 24) | ((WORD)data[j + 1] << 16) |
        ((WORD)data[j + 2] << 8) | ((WORD)data[j + 3]);
```

## c/src/dml_msgpack.c

MessagePack encode/decode for `DmlValue` and custom extension types.

### dml_msgpack_entry_compare

API: `static int dml_msgpack_entry_compare(const void *left, const void *right)`
Map-key comparator for deterministic packing.

### dml_msgpack_pack_value

API: `static int dml_msgpack_pack_value(msgpack_packer *packer, const DmlValue *value)`
Packs a `DmlValue` into msgpack bytes.

### dml_msgpack_pack

API: `int dml_msgpack_pack(const DmlValue *value, DmlMsgpackBuffer *out_buffer)`
Encodes a `DmlValue` into a heap-owned buffer.

### dml_msgpack_from_object

API: `static DmlValue *dml_msgpack_from_object(const msgpack_object *obj)`
Decodes a msgpack object into a `DmlValue`.

### dml_msgpack_unpack

API: `int dml_msgpack_unpack(const char *data, size_t size, DmlValue **out_value)`
Decodes msgpack bytes into a `DmlValue`.

### dml_msgpack_free_buffer

API: `void dml_msgpack_free_buffer(void *data)`
Frees buffers returned from `dml_msgpack_pack`.

## c/src/dml_value.c

Value construction, manipulation, and ref parsing utilities.

### dml_value_map_entry_compare

API: `static int dml_value_map_entry_compare(const void *left, const void *right)`
Key comparator used for map sorting.

### dml_value_alloc

API: `static DmlValue *dml_value_alloc(DmlValueType type)`
Allocates and initializes a `DmlValue` container.

### dml_value_new_null

API: `DmlValue *dml_value_new_null(void)`
Constructs a null value.

### dml_value_new_bool

API: `DmlValue *dml_value_new_bool(int value)`
Constructs a boolean value.

### dml_value_new_int

API: `DmlValue *dml_value_new_int(long long value)`
Constructs an integer value.

### dml_value_new_float

API: `DmlValue *dml_value_new_float(double value)`
Constructs a float value.

### dml_value_new_str

API: `DmlValue *dml_value_new_str(const char *data, size_t size)`
Constructs a string value with copied bytes.

### dml_value_new_ref

API: `DmlValue *dml_value_new_ref(const char *data, size_t size)`
Constructs a ref value.

### dml_value_new_resource

API: `DmlValue *dml_value_new_resource(const char *uri, size_t uri_len, DmlValue *data, const char *adapter, size_t adapter_len)`
Constructs a resource value.

### dml_value_new_list

API: `DmlValue *dml_value_new_list(size_t count)`
Constructs a list container.

### dml_value_list_set

API: `int dml_value_list_set(DmlValue *list, size_t index, DmlValue *item)`
Assigns a list element.

### dml_value_new_map

API: `DmlValue *dml_value_new_map(size_t count)`
Constructs a map container.

### dml_value_map_set

API: `int dml_value_map_set(DmlValue *map, size_t index, const char *key, size_t key_len, DmlValue *value)`
Assigns a map entry.

### dml_value_map_sort

API: `int dml_value_map_sort(DmlValue *map)`
Sorts map entries by key for deterministic ordering.

### dml_value_free

API: `void dml_value_free(DmlValue *value)`
Frees a `DmlValue` tree and its owned memory.

### dml_ref_split

API: `int dml_ref_split(const char *ref, size_t ref_len, const char **namespace_str, size_t *namespace_len, const char **id_str, size_t *id_len)`
Splits a `namespace/id` ref into components.

## c/bindings/cpython/py_module.c

CPython extension module exposing the C core to Python.

### DML_DB_CAPSULE_NAME

API: `#define DML_DB_CAPSULE_NAME`
Capsule name string for `DmlDbHandle` objects.

### DML_DB_TXN_CAPSULE_NAME

API: `#define DML_DB_TXN_CAPSULE_NAME`
Capsule name string for `DmlDbTxn` objects.

### RefObject

API: `typedef struct RefObject`
Python object type backing `Ref`, holding a `to` string.

### RefType

API: `static PyTypeObject RefType`
Global `PyTypeObject` descriptor for `Ref`.

### ResourceObject

API: `typedef struct ResourceObject`
Python object type backing `Resource`, holding `uri`, `data`, and `adapter`.

### ResourceType

API: `static PyTypeObject ResourceType`
Global `PyTypeObject` descriptor for `Resource`.

### py_to_dml_value

API: `static DmlValue *py_to_dml_value(PyObject *obj)`
Converts Python values into `DmlValue` trees.

### dml_value_to_py

API: `static PyObject *dml_value_to_py(const DmlValue *value)`
Converts `DmlValue` trees into Python objects.

### ref_new_from_to

API: `static PyObject *ref_new_from_to(PyObject *to_obj)`
Allocates a `Ref` from a Python string.

### resource_new_from_parts

API: `static PyObject *resource_new_from_parts(PyObject *uri_obj, PyObject *data_obj, PyObject *adapter_obj)`
Allocates a `Resource` from Python objects.

### raise_lmdb_error

API: `static PyObject *raise_lmdb_error(int rc, const char *context)`
Formats LMDB errors into `RuntimeError`.

### db_capsule_destructor

API: `static void db_capsule_destructor(PyObject *capsule)`
Frees a `DmlDbHandle` when a capsule is destroyed.

### db_txn_capsule_destructor

API: `static void db_txn_capsule_destructor(PyObject *capsule)`
Aborts a `DmlDbTxn` on capsule teardown.

### raise_runtime_error

API: `static PyObject *raise_runtime_error(const char *message)`
Raises `RuntimeError`.

### db_handle_from_capsule

API: `static DmlDbHandle *db_handle_from_capsule(PyObject *capsule)`
Extracts a handle pointer.

### raise_type_error

API: `static PyObject *raise_type_error(const char *message)`
Raises `TypeError`.

### raise_value_error

API: `static PyObject *raise_value_error(const char *message)`
Raises `ValueError`.

### raise_db_error

API: `static PyObject *raise_db_error(int rc, const char *context)`
Maps core error codes to Python exceptions.

### parse_namespace_list

API: `static int parse_namespace_list(PyObject *obj, const char ***out_names, size_t *out_count)`
Validates and extracts namespace lists.

### py_hash_sha256_hex

API: `static PyObject *py_hash_sha256_hex(PyObject *self, PyObject *args)`
Exposes `dml_hash_sha256_hex` to Python.

### ref_new

API: `static PyObject *ref_new(PyTypeObject *type, PyObject *args, PyObject *kwds)`
`Ref` constructor.

### ref_dealloc

API: `static void ref_dealloc(RefObject *self)`
`Ref` destructor.

### ref_repr

API: `static PyObject *ref_repr(RefObject *self)`
`Ref` representation as `Ref(<ns>/<id>)`.

### resource_new

API: `static PyObject *resource_new(PyTypeObject *type, PyObject *args, PyObject *kwds)`
`Resource` constructor.

### resource_dealloc

API: `static void resource_dealloc(ResourceObject *self)`
`Resource` destructor.

### resource_repr

API: `static PyObject *resource_repr(ResourceObject *self)`
`Resource` representation as `Resource(<uri>, <adapter>)`.

### resource_richcompare

API: `static PyObject *resource_richcompare(PyObject *a, PyObject *b, int op)`
Equality comparison for resources.

### ref_richcompare

API: `static PyObject *ref_richcompare(PyObject *a, PyObject *b, int op)`
Equality comparison for refs.

### ref_ns

API: `static PyObject *ref_ns(RefObject *self, PyObject *args)`
Returns the namespace portion of a ref.

### ref_id

API: `static PyObject *ref_id(RefObject *self, PyObject *args)`
Returns the id portion of a ref.

### ref_members

API: `static PyMemberDef ref_members[]`
Defines `Ref.to` as a read-only member.

### ref_methods

API: `static PyMethodDef ref_methods[]`
Defines `Ref.ns()` and `Ref.id()`.

### resource_members

API: `static PyMemberDef resource_members[]`
Defines `Resource.uri`, `Resource.data`, `Resource.adapter` members.

### py_db_create

API: `static PyObject *py_db_create(PyObject *self, PyObject *args)`
Creates a DB handle capsule.

### py_db_open

API: `static PyObject *py_db_open(PyObject *self, PyObject *args)`
Opens a DB handle capsule.

### py_db_put

API: `static PyObject *py_db_put(PyObject *self, PyObject *args)`
Inserts a JSON-serializable value and returns a `Ref`.

### py_db_get

API: `static PyObject *py_db_get(PyObject *self, PyObject *args)`
Reads a value by `Ref`.

### py_db_dump_ref

API: `static PyObject *py_db_dump_ref(PyObject *self, PyObject *args)`
Dumps a ref graph to bytes.

### py_db_load_ref

API: `static PyObject *py_db_load_ref(PyObject *self, PyObject *args)`
Loads a ref graph from bytes.

### py_db_insert

API: `static PyObject *py_db_insert(PyObject *self, PyObject *args)`
Inserts raw MessagePack bytes and returns a `Ref`.

### py_db_resize

API: `static PyObject *py_db_resize(PyObject *self, PyObject *args)`
Resizes the LMDB map.

### py_db_mapsize

API: `static PyObject *py_db_mapsize(PyObject *self, PyObject *args)`
Returns map size.

### py_db_iter

API: `static PyObject *py_db_iter(PyObject *self, PyObject *args, PyObject *kwargs)`
Lists keys with optional namespace/start token.

### py_db_txn_begin

API: `static PyObject *py_db_txn_begin(PyObject *self, PyObject *args, PyObject *kwargs)`
Begins a transaction.

### py_db_txn_commit

API: `static PyObject *py_db_txn_commit(PyObject *self, PyObject *args)`
Commits a transaction.

### py_db_txn_abort

API: `static PyObject *py_db_txn_abort(PyObject *self, PyObject *args)`
Aborts a transaction.

### py_db_close

API: `static PyObject *py_db_close(PyObject *self, PyObject *args)`
Closes a DB handle.

### py_db_reopen

API: `static PyObject *py_db_reopen(PyObject *self, PyObject *args)`
Reopens a DB handle.

### py_msgpack_pack

API: `static PyObject *py_msgpack_pack(PyObject *self, PyObject *args)`
Packs a Python value into MessagePack bytes.

### py_msgpack_unpack

API: `static PyObject *py_msgpack_unpack(PyObject *self, PyObject *args)`
Unpacks MessagePack bytes into Python values.

### CoreMethods

API: `static PyMethodDef CoreMethods[]`
Method table for the `_core` module.

### coremodule

API: `static struct PyModuleDef coremodule`
Module definition for `_core`.

### PyInit\_\_core

API: `PyMODINIT_FUNC PyInit__core(void)`
Initializes the `_core` Python module and type objects.
