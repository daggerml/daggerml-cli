# Module Import Graph

This document describes the import relationships between modules in the `daggerml_cli` package.

## Mermaid Chart

```mermaid
graph TD
    subgraph "Top-level modules"
        init["__init__"] --> db["_db"]
        init --> dml["dml"]
        init --> types["types"]
        dml --> db
        dml --> base_ops["ops.base_ops"]
        dml --> cache["ops.cache"]
        dml --> commit["ops.commit"]
        dml --> dag["ops.dag"]
        dml --> gc["ops.gc"]
        dml --> head["ops.head"]
        dml --> index["ops.index"]
        dml --> node["ops.node"]
        dml --> types
        types --> db
        types --> util["util"]
        builtins["builtins"] --> types
        builtins --> util
        python_fork_adapter["python_fork_adapter"]
        about["__about__"]
        db
    end

    subgraph "ops submodule"
        ops_init["__init__"]
        base_ops --> db
        base_ops --> types
        cache --> db
        cache --> base_ops
        cache --> types
        commit --> db
        commit --> base_ops
        commit --> types
        commit --> util
        dag --> db
        dag --> base_ops
        dag --> types
        gc --> db
        gc --> base_ops
        gc --> types
        head --> db
        head --> base_ops
        head --> types
        index --> db
        index --> builtins
        index --> base_ops
        index --> cache
        index --> dag
        index --> node
        index --> types
        index --> util
        node --> db
        node --> base_ops
        node --> types
    end
```

## Detailed Description

### Top-level modules

### daggerml_cli.__init__
Imports:
- daggerml_cli._db (Ref, Resource)
- daggerml_cli.dml (Dml)
- daggerml_cli.types (...)

### daggerml_cli.dml
Imports:
- daggerml_cli._db (DmlDbEnv, Ref)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.ops.cache (CacheOps)
- daggerml_cli.ops.commit (CommitOps)
- daggerml_cli.ops.dag (DagOps)
- daggerml_cli.ops.gc (GcOps)
- daggerml_cli.ops.head (HeadOps)
- daggerml_cli.ops.index (IndexOps)
- daggerml_cli.ops.node (NodeOps)
- daggerml_cli.types (...)

### daggerml_cli.types
Imports:
- daggerml_cli._db (Ref, Resource)
- daggerml_cli.util (now)

### daggerml_cli.util
Imports: None

### daggerml_cli.builtins
Imports:
- daggerml_cli.types (NONE)
- daggerml_cli.util (unnest)

### daggerml_cli.python_fork_adapter
Imports: None

### daggerml_cli._db
Imports: None

### daggerml_cli.__about__
Imports: None

## ops submodule

### daggerml_cli.ops.__init__
Imports: None

### daggerml_cli.ops.base_ops
Imports:
- daggerml_cli._db (...)
- daggerml_cli.types (...)

### daggerml_cli.ops.cache
Imports:
- daggerml_cli._db (...)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.types (Cache, DmlRepoError)

### daggerml_cli.ops.commit
Imports:
- daggerml_cli._db (Ref)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.types (...)
- daggerml_cli.util (now)

### daggerml_cli.ops.dag
Imports:
- daggerml_cli._db (Ref)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.types (DmlRepoError)

### daggerml_cli.ops.gc
Imports:
- daggerml_cli._db (Ref)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.types (DmlRepoError)

### daggerml_cli.ops.head
Imports:
- daggerml_cli._db (Ref)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.types (DmlRepoError, Head)

### daggerml_cli.ops.index
Imports:
- daggerml_cli._db (Ref, Resource)
- daggerml_cli.builtins (BUILTIN_FNS)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.ops.cache (CacheOps)
- daggerml_cli.ops.dag (DagOps)
- daggerml_cli.ops.node (NodeOps)
- daggerml_cli.types (...)
- daggerml_cli.util (now)

### daggerml_cli.ops.node
Imports:
- daggerml_cli._db (Ref)
- daggerml_cli.ops.base_ops (BaseOps)
- daggerml_cli.types (Datum, DmlRepoError, Node)