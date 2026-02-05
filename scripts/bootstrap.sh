#!/usr/bin/env bash
set -euo pipefail

OS="$(uname -s)"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage: ./scripts/bootstrap.sh

Installs memory-check prerequisites for the current OS.

Environment variables:
  PYENV_VERSION   Python version to install with pyenv (default: 3.11.8)
  PYENV_ROOT      pyenv root directory (default: ~/.pyenv)

Notes:
  - On macOS, installs pyenv + a non-hardened Python for ASan.
  - On Linux (apt), installs clang and valgrind.
EOF
  exit 0
fi

if [[ "${OS}" == "Darwin" ]]; then
  if ! command -v brew >/dev/null 2>&1; then
    echo "Homebrew is required on macOS. Install it from https://brew.sh/" >&2
    exit 1
  fi

  echo "Installing prerequisites via Homebrew..."
  brew update
  brew install llvm pyenv openssl@3 readline sqlite3 xz zlib

  PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
  export PYENV_ROOT
  export PATH="${PYENV_ROOT}/bin:${PYENV_ROOT}/shims:${PATH}"

  if ! command -v pyenv >/dev/null 2>&1; then
    echo "pyenv is required on macOS. Ensure Homebrew installed it correctly." >&2
    exit 1
  fi

  PYENV_VERSION="${PYENV_VERSION:-3.11.8}"
  OPENSSL_PREFIX="$(brew --prefix openssl@3)"
  READLINE_PREFIX="$(brew --prefix readline)"
  ZLIB_PREFIX="$(brew --prefix zlib)"

  echo "Installing Python ${PYENV_VERSION} via pyenv..."
  env \
    CPPFLAGS="-I${OPENSSL_PREFIX}/include -I${READLINE_PREFIX}/include -I${ZLIB_PREFIX}/include" \
    LDFLAGS="-L${OPENSSL_PREFIX}/lib -L${READLINE_PREFIX}/lib -L${ZLIB_PREFIX}/lib" \
    PYTHON_CONFIGURE_OPTS="--enable-shared" \
    pyenv install -s "${PYENV_VERSION}"

  pyenv rehash
  PYENV_VERSION="${PYENV_VERSION}" pyenv exec python -m pip install --upgrade pip
  PYENV_VERSION="${PYENV_VERSION}" pyenv exec python -m pip install --upgrade uv
elif [[ "${OS}" == "Linux" ]]; then
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y python3 python3-venv python3-pip clang lld valgrind
  else
    echo "Unsupported Linux package manager. Install python3, clang, and valgrind manually." >&2
    exit 1
  fi

  echo "Installing uv..."
  python3 -m pip install --upgrade uv
else
  echo "Unsupported OS: ${OS}" >&2
  exit 1
fi
