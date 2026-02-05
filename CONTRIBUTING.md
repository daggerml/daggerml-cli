# Contributing to DaggerML CLI

Thank you for your interest in contributing! We welcome contributions via pull
requests and appreciate your help in improving this project.

## Reporting Issues

- Search [existing issues](https://github.com/daggerml/daggerml-cli/issues) before submitting a new one.
- When reporting a bug, please include:
  - A clear, descriptive title.
  - Steps to reproduce the issue.
  - Expected and actual behavior.
  - Python version and operating system.
  - Relevant code snippets or error messages.

## How to Contribute Code

### **Want to dive in?**

1. Find an issue you'd like to tackle from GitHub [Issues](https://github.com/daggerml/daggerml-cli/issues)
2. Check out the _Assignees_ section on the issue tracker page. If nobody is already assigned, feel free to assign yourself. Otherwise message the assignee first to coordinate.
3. Click the "create a branch" link from within the issue tracker to create a branch for this particular issue.
4. Clone the repository and set it up:
   ```bash
   git clone https://github.com/daggerml/daggerml-cli.git
   ```
5. Make your changes in the new branch.
6. Write or update tests as needed.
7. Ensure all tests pass locally.
8. Commit and push changes to that issue branch.
9. Once your code is ready to go, rebase off of master and create a pull request and set @amniskin as the approver. DO NOT MERGE TO MASTER.

## Coding Standards

- Follow [PEP 8](https://pep8.org/) for Python code style.
- Use [numpy style docstrings](https://numpydoc.readthedocs.io/en/latest/format.html) for all public modules, classes, functions, and methods.
- Write clear, concise commit messages.
- Keep pull requests focused and minimal.
- Collapse intra‑function vertical whitespace; keep only the blank lines that separate top‑level blocks (imports/class/def), not within a given body.

### C Code Standards

- All C code logic should reside in the `c/src` directory and should not include any Python headers.
- The only file that should include `Python.h` is `c/bindings/cpython/py_module.c`.
- Any bindings should only serve as thin wrappers around the core C logic -- only whatever is necessary to expose the functionality to Python.

## Vendored Dependencies (C/C++)

When we vendor third-party C/C++ libraries, keep the footprint minimal and include only what is required to build plus the upstream licenses. Avoid committing source tarballs or extra tooling files. Include a README.md in the vendored directory with the following information:

- Source URL of the upstream project.
- Specific version or commit hash used.
- Steps taken to extract and vendor the code.

## Testing Guidelines

- Add or update unit tests for any new features or bug fixes.
- Use [pytest](https://pytest.org/) for running tests.
- The testing requirements are included in the `test` feature for the library.
  - Run tests with [uv](https://uv.run/):
    ```sh
    uv run --dev pytest .
    ```
  - If you're using vscode, you can create a venv with the `dev` dependencies and run tests with the command palette:
    ```
    Python: Run Tests
    ```
  - Or install the `dev` dependencies with uv and run tests:
    ```
    uv sync --dev
    uv run --dev pytest .
    ```
- Run all tests locally before submitting a pull request.
- Ensure your code passes all tests and does not decrease code coverage.
- If your changes introduce new dependencies, please update `pyproject.toml`.

Thank you for helping make this project better!

## Rebuilding the C Extension

```bash
export SKBUILD_CMAKE_ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo"
rm -rf .venv
uv venv
uv pip install -e .
```

## Memory Testing

Run the full memory test suite (ASan/UBSan, then Valgrind when available):

```bash
./scripts/bootstrap.sh
./scripts/memcheck.sh
```

Notes:

- On macOS, `./scripts/bootstrap.sh` installs a pyenv Python (non-hardened) so ASan/UBSan can preload.
