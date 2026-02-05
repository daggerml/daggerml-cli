.PHONY: help test memcheck clean

TEST ?= .

help:
	@echo "Commands:"
	@echo "  test      - Run tests"
	@echo "  memcheck  - Run full suite with ASan/UBSan and Valgrind (Linux only)"
	@echo "  clean     - Remove build artifacts"

test:
	@if [ -n "$(UV_PROJECT_ENVIRONMENT)" ]; then \
		echo "Using UV_PROJECT_ENVIRONMENT: $(UV_PROJECT_ENVIRONMENT)"; \
		export VIRTUAL_ENV=$(UV_PROJECT_ENVIRONMENT); \
	fi; \
	uv run --dev pytest -v ${TEST}

memcheck:
	./scripts/memcheck.sh


clean:
	@echo "Cleaning build artifacts..."
	rm -rf _skbuild build
