# daggerml-cli [![PyPI - Version](https://img.shields.io/pypi/v/daggerml-cli.svg)](https://pypi.org/project/daggerml-cli) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/daggerml-cli.svg)](https://pypi.org/project/daggerml-cli)

![The Prince of DAGness](img/prince-of-dagness.jpg)


## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Datums](#datums)
- [Test](#test)
- [Memory Testing](#memory-testing)
- [Build](#build)
- [License](#license)

## Install

```sh
pipx install daggerml-cli
```

## Usage

```sh
dml --help
dml COMMAND --help
dml COMMAND SUBCOMMAND --help
```

> [!TIP]
> Shell completion is available for bash/zsh via [argcomplete](https://github.com/kislyuk/argcomplete).

## Runtime Notes

- On startup, check the machine endianness (`sys.byteorder`) to ensure it is little-endian.

## Datums

- See `docs/DATUMS.md` for supported datum types and restrictions.


## Test

```sh
uv run --dev pytest .
```

## Memory Testing

Bootstrap prerequisites, then run the full test suite with AddressSanitizer + UndefinedBehaviorSanitizer, then Valgrind (Linux only):

```sh
./scripts/bootstrap.sh
./scripts/memcheck.sh
```

Notes:
- On macOS, `./scripts/bootstrap.sh` installs a pyenv Python (non-hardened) so ASan/UBSan can preload.

## Build

```sh
uv pip install -e .
```

## License

`daggerml-cli` is distributed under the terms of the [MIT](LICENSE.txt) license.
