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
2. Check out the *Assignees* section on the issue tracker page. If nobody is already assigned, feel free to assign yourself. Otherwise message the assignee first to coordinate.
3. Click the "create a branch" link from within the issue tracker to create a branch for this particular issue.
4. Clone the repository and set it up:
   ```bash
   git clone https://github.com/daggerml/daggerml-cli.git
   ```
5. Make your changes in the new branch.
6. Write or update tests as needed.
7. Ensure all tests pass locally.
8. Commit and push changes to that issue branch.
8. Once your code is ready to go, rebase off of master and create a pull request and set @amniskin as the approver. DO NOT MERGE TO MASTER.

## Coding Standards

- Follow [PEP 8](https://pep8.org/) for Python code style.
- Use [numpy style docstrings](https://numpydoc.readthedocs.io/en/latest/format.html) for all public modules, classes, functions, and methods.
- Write clear, concise commit messages.
- Keep pull requests focused and minimal.

## Testing Guidelines

- Add or update unit tests for any new features or bug fixes.
- Use [pytest](https://pytest.org/) for running tests.
- The testing requirements are included in the `test` feature for the library.
  - You can run tests using [hatch](https://hatch.pypa.io/):  
    ```
    hatch run pytest .
    ```
  - If you're using vscode, you can create a venv with the `test` feature and run tests with the command palette:
    ```
    Python: Run Tests
    ```
  - Or install the `test` feature with pip and run tests:  
    ```
    pip install -e </path/to/daggerml-cli>[test]
    pytest .
    ```
- Run all tests locally before submitting a pull request.
- Ensure your code passes all tests and does not decrease code coverage.
- If your changes introduce new dependencies, please update `pyproject.toml`.

Thank you for helping make this project better!
