# C Return Codes and Custom Exceptions

This document describes the C return codes from the dml_core library and the custom Python exceptions used in the DML repository system.

## C Return Codes

From dml_core, the following return codes are used:

### Success
- `DML_DB_OK` (0): Operation successful

### Handle and Transaction State
- `DML_DB_ERR_HANDLE_INVALID`: Handle pointer is NULL or otherwise not initialized
- `DML_DB_ERR_HANDLE_CLOSED`: Handle was closed before the operation
- `DML_DB_ERR_HANDLE_FORKED`: Handle used after fork without reopen
- `DML_DB_ERR_TXN_INVALID`: Transaction pointer invalid or closed
- `DML_DB_ERR_TXN_READONLY`: Operation attempted to write in a read-only transaction
- `DML_DB_ERR_TXN_FORKED`: Transaction used after fork without reopen
- `DML_DB_ERR_ENV_REOPENED`: Environment was reopened; all transactions invalidated (retry required)

### Inputs and References
- `DML_DB_ERR_INPUT_INVALID`: Generic bad input (null pointers, empty strings where disallowed)
- `DML_DB_ERR_TYPE_INVALID`: Input type is not representable in the database
- `DML_DB_ERR_PATH_INVALID`: Database path is missing or inaccessible
- `DML_DB_ERR_REF_INVALID`: Ref parsing failed or invalid reference format
- `DML_DB_ERR_NAMESPACE_INVALID`: Namespace not configured or not allowed

### Data and Encoding
- `DML_DB_ERR_NOT_FOUND`: Key/data pair does not exist
- `DML_DB_ERR_KEY_EXISTS`: Key already exists and overwrite is disallowed
- `DML_DB_ERR_MSGPACK`: MessagePack encode/decode failure

### Resources and Capacity
- `DML_DB_ERR_NOMEM`: Allocation failed
- `DML_DB_ERR_MAP_FULL`: LMDB map is full (resize needed)
- `DML_DB_ERR_BUSY`: Mutex/lock acquisition failure

### Backend Failures
- `DML_DB_ERR_LMDB`: Underlying LMDB failure not covered by specific codes
- `DML_DB_ERR_INTERNAL`: Unexpected internal failure (invariant broken)

## Python Exception Hierarchy

### Base Exceptions
- `DmlError`: Base exception for all DML errors
- `DmlDbError(DmlError)`: Base for database-related errors
- `DmlRepoError(Exception)`: Exception raised by DML repository operations

### Database Exceptions (all inherit from DmlDbError)
- `DmlDbInvalidHandleError`
- `DmlDbClosedError`
- `DmlDbForkedError`
- `DmlDbInvalidTxnError`
- `DmlDbReadonlyTxnError`
- `DmlDbForkedTxnError`
- `DmlDbEnvReopenedError`
- `DmlDbInvalidInputError(ValueError, DmlDbError)`
- `DmlDbInvalidTypeError(ValueError, DmlDbError)`
- `DmlDbInvalidPathError(ValueError, DmlDbError)`
- `DmlDbInvalidRefError(ValueError, DmlDbError)`
- `DmlDbInvalidNamespaceError(ValueError, DmlDbError)`
- `DmlDbKeyNotFoundError(DmlDbError)`
- `DmlDbKeyExistsError(DmlDbError)`
- `DmlDbMsgpackError(DmlDbError)`
- `DmlDbOutOfMemoryError(MemoryError, DmlDbError)`
- `DmlDbMapFullError(DmlDbError)`
- `DmlDbBusyError(DmlDbError)`
- `DmlDbLmdbError(DmlDbError)`
- `DmlDbInternalError(DmlDbError)`

## Return Code to Exception Mapping

Each C return code maps to a specific Python exception:

- `DML_DB_ERR_HANDLE_INVALID` → `DmlDbInvalidHandleError`
- `DML_DB_ERR_HANDLE_CLOSED` → `DmlDbClosedError`
- `DML_DB_ERR_HANDLE_FORKED` → `DmlDbForkedError`
- `DML_DB_ERR_TXN_INVALID` → `DmlDbInvalidTxnError`
- `DML_DB_ERR_TXN_READONLY` → `DmlDbReadonlyTxnError`
- `DML_DB_ERR_TXN_FORKED` → `DmlDbForkedTxnError`
- `DML_DB_ERR_ENV_REOPENED` → `DmlDbEnvReopenedError`
- `DML_DB_ERR_INPUT_INVALID` → `DmlDbInvalidInputError`
- `DML_DB_ERR_TYPE_INVALID` → `DmlDbInvalidTypeError`
- `DML_DB_ERR_PATH_INVALID` → `DmlDbInvalidPathError`
- `DML_DB_ERR_REF_INVALID` → `DmlDbInvalidRefError`
- `DML_DB_ERR_NAMESPACE_INVALID` → `DmlDbInvalidNamespaceError`
- `DML_DB_ERR_NOT_FOUND` → `DmlDbKeyNotFoundError`
- `DML_DB_ERR_KEY_EXISTS` → `DmlDbKeyExistsError`
- `DML_DB_ERR_MSGPACK` → `DmlDbMsgpackError`
- `DML_DB_ERR_NOMEM` → `DmlDbOutOfMemoryError`
- `DML_DB_ERR_MAP_FULL` → `DmlDbMapFullError`
- `DML_DB_ERR_BUSY` → `DmlDbBusyError`
- `DML_DB_ERR_LMDB` → `DmlDbLmdbError`
- `DML_DB_ERR_INTERNAL` → `DmlDbInternalError`

## Troubleshooting & Recovery Architecture

### LMDB Handle Staleness (`EINVAL` / `-18`)

A common issue encountered in Linux CI environments and parallel test execution is LMDB returning `EINVAL` (error code 22). This corresponds to an invalid or stale `MDB_env*` handle.

**Root Cause:**
This error occurs when an `MDB_env*` handle becomes stale or invalid. This frequently happens when:
1.  A process forks (LMDB handles cannot be shared across forks).
2.  Aggressive temporary file cleanup invalidates the underlying memory map.
3.  Parallel tests or multiple processes interfere with the environment state.

**Recovery Architecture:**

The system implements a coordinated two-tier recovery strategy:

1.  **C-Level Environment Repair:**
    *   `dml_db_validate()` detects stale environments by checking for `EINVAL` from `mdb_env_stat()` or fork detection via PID tracking.
    *   When detected with `reopen=1`, it automatically calls `dml_db_reopen_handle()` to:
        - Deep-copy the configuration
        - Safely close the old handle
        - Open a fresh `MDB_env`
    *   After successful reopen, it returns `DML_DB_ERR_ENV_REOPENED` to signal: "**The environment is now fixed, but all existing transactions are invalid - caller must retry**."
    *   Functions that can start fresh transactions (like `dml_db_txn_begin`) treat `ENV_REOPENED` as success and proceed to create a new transaction.
    *   Functions with existing transactions (like `dml_db_put`, `dml_db_get`) propagate `ENV_REOPENED` up to Python, since the current transaction is now invalid.

2.  **Python-Level Transaction Retry:**
    *   The `with_retry` decorator (in `base_ops.py`) wraps transaction-based operations.
    *   When `DmlDbEnvReopenedError` is raised (mapped from `DML_DB_ERR_ENV_REOPENED`), the decorator:
        - Logs the environment reopen event
        - Retries the **entire transaction block** from scratch
        - On retry, `dml_db_txn_begin` will succeed with the repaired environment
    *   This ensures atomicity: either the full transaction succeeds, or it retries completely after environment repair.
    *   The decorator also handles `DmlDbMapFullError` for automatic database resizing.

**Key Invariants:**
- The environment is **immediately repaired** when staleness is detected (if `reopen=1`).
- The error code `ENV_REOPENED` means: "Environment is good; transactions are dead; retry your operation."
- Python retry logic ensures transaction atomicity across environment repairs.
- Maximum retry attempts prevent infinite loops (8 attempts for both resize and reopen scenarios).