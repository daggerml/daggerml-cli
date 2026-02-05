#!/usr/bin/env bash
set -euo pipefail

OS="$(uname -s)"

LOG_DIR="scripts"
mkdir -p "$LOG_DIR"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage: ./scripts/memcheck.sh

Runs ASan/UBSan tests and Valgrind (Linux only).

Environment variables:
  PYENV_VERSION   pyenv Python version for macOS (default: 3.11.8)
  PYENV_ROOT      pyenv root directory (default: ~/.pyenv)
  UV_PYTHON       Python interpreter for uv (overrides pyenv selection)
  SKBUILD_CMAKE_ARGS  Extra CMake args for scikit-build-core

Notes:
  - On macOS, requires a pyenv-built Python for ASan preload.
  - Valgrind runs only on Linux.
EOF
  exit 0
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Run ./scripts/bootstrap.sh first." >&2
  exit 1
fi

if ! command -v clang >/dev/null 2>&1; then
  echo "clang is required. Install LLVM toolchain first." >&2
  exit 1
fi

if [[ "${OS}" == "Linux" ]] && ! command -v valgrind >/dev/null 2>&1; then
  echo "valgrind is required on Linux. Install it first." >&2
  exit 1
fi

PYENV_VERSION="${PYENV_VERSION:-3.11.8}"
PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
PYENV_PYTHON="${PYENV_ROOT}/versions/${PYENV_VERSION}/bin/python"

rm -rf _skbuild

if [[ "${OS}" == "Darwin" ]]; then
  if [[ ! -x "${PYENV_PYTHON}" ]]; then
    echo "pyenv Python ${PYENV_VERSION} is required on macOS." >&2
    echo "Run ./scripts/bootstrap.sh to install it." >&2
    exit 1
  fi

  if [[ -z "${UV_PYTHON:-}" ]]; then
    export UV_PYTHON="${PYENV_PYTHON}"
  fi
  export ASAN_DYLIB="$(clang -print-file-name=libclang_rt.asan_osx_dynamic.dylib)"
  if [[ -z "${ASAN_DYLIB}" || "${ASAN_DYLIB}" == "libclang_rt.asan_osx_dynamic.dylib" ]]; then
    echo "ASan runtime dylib not found via clang." >&2
    exit 1
  fi
  export DYLD_INSERT_LIBRARIES="${ASAN_DYLIB}"
  export DYLD_FORCE_FLAT_NAMESPACE=1
  export ASAN_OPTIONS="detect_leaks=0:halt_on_error=1:abort_on_error=1:symbolize=1:log_path=${LOG_DIR}/asan"
  export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
else
  export ASAN_OPTIONS="detect_leaks=1:halt_on_error=1:abort_on_error=1:symbolize=1"
  export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
fi

echo "=== ASan/UBSan ==="
# capture uv sync/install logs
SKBUILD_CMAKE_ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo;-DDML_ENABLE_ASAN=ON;-DDML_ENABLE_UBSAN=ON" uv sync --dev >"${LOG_DIR}/uv_sync.log" 2>&1
SKBUILD_CMAKE_ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo;-DDML_ENABLE_ASAN=ON;-DDML_ENABLE_UBSAN=ON" uv pip install -e . >"${LOG_DIR}/uv_install.log" 2>&1

if [[ "${OS}" == "Darwin" ]]; then
  VENV_PYTHON=".venv/bin/python"
  if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "Expected ${VENV_PYTHON} to exist after uv sync." >&2
    exit 1
  fi
  # Run pytest under the venv python and capture ASan output to scripts/asan_pytest.log
  DYLD_INSERT_LIBRARIES="${DYLD_INSERT_LIBRARIES}" \
    DYLD_FORCE_FLAT_NAMESPACE="${DYLD_FORCE_FLAT_NAMESPACE}" \
    ASAN_OPTIONS="${ASAN_OPTIONS}" \
    UBSAN_OPTIONS="${UBSAN_OPTIONS}" \
    "${VENV_PYTHON}" -X faulthandler -m pytest -v . 2>&1 | tee "${LOG_DIR}/asan_pytest.log"
else
  # Linux: run pytest under uv run --dev and capture output
  SKBUILD_CMAKE_ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo;-DDML_ENABLE_ASAN=ON;-DDML_ENABLE_UBSAN=ON" env ASAN_OPTIONS="${ASAN_OPTIONS}" UBSAN_OPTIONS="${UBSAN_OPTIONS}" uv run --dev pytest -v . 2>&1 | tee "${LOG_DIR}/asan_pytest.log"
fi


echo "=== Valgrind ==="
rm -rf _skbuild
SKBUILD_CMAKE_ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo" uv sync --dev >"${LOG_DIR}/valgrind_sync.log" 2>&1
SKBUILD_CMAKE_ARGS="-DCMAKE_BUILD_TYPE=RelWithDebInfo" uv pip install -e . >"${LOG_DIR}/valgrind_install.log" 2>&1
if [[ "${OS}" == "Linux" ]]; then
  valgrind --tool=memcheck \
    --leak-check=full \
    --show-leak-kinds=all \
    --track-origins=yes \
    --error-exitcode=1 \
    env uv run --dev pytest -v . 2>&1 | tee "${LOG_DIR}/valgrind.log"
else
  echo "Valgrind is not supported on macOS; skipping." | tee "${LOG_DIR}/valgrind.log"
fi
