PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=n6k_client
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Stage the coi (wasm_threads) extension into the npm package. coi is the only
# wasm variant we build/ship — eh/mvp can't load a shared-memory extension, and
# n6k mandates cross-origin isolation, so the coi bundle is always the runtime.
npm_db_wasm:
	rm -rf packages/npm/wasm
	mkdir -p packages/npm/wasm
	cp -r build/wasm_threads/repository/* packages/npm/wasm/

# The coi (wasm_threads) extension must be built with the emsdk that matches
# duckdb-wasm's coi runtime. A different emcc silently produces an incompatible
# side-module, so gate the build on the pinned version.
EXPECTED_EMCC_VERSION := 3.1.71

# Preflight: fail fast unless the emcc on PATH is the pinned toolchain.
.PHONY: wasm-preflight
wasm-preflight:
	@command -v emcc >/dev/null 2>&1 || { echo "ERROR: emcc not on PATH (activate emsdk $(EXPECTED_EMCC_VERSION))"; exit 1; }
	@emcc --version | grep -q " $(EXPECTED_EMCC_VERSION) " || { echo "ERROR: emcc must be $(EXPECTED_EMCC_VERSION), got: $$(emcc --version | head -1)"; exit 1; }
	@echo "emcc $(EXPECTED_EMCC_VERSION) OK"

# The vendored wasm_threads target compiles objects with -pthread but never
# passes -DUSE_WASM_THREADS=1, so duckdb leaves WASM_THREAD_FLAGS empty and the
# -sSIDE_MODULE=2 link omits -sSHARED_MEMORY=1 — producing extensions that
# import a NON-shared memory and fail to load against the shared-memory coi
# runtime (LinkError: mismatch in shared state of memory). Inject the flag via
# EXT_FLAGS (the submodule's documented cmake passthrough) so we don't have to
# patch the submodule. Set on wasm_threads itself so a direct build is covered.
wasm_threads: EXT_FLAGS += -DUSE_WASM_THREADS=1

# The vendored WASM_CXX_THREADS_FLAGS references the undefined
# WASM_COMPILE_TIME_EH_FLAGS, so wasm_threads builds without -fwasm-exceptions:
# a throw then skips every catch and destructor in the extension's frames
# (a lock_guard held across a throw stays locked forever).
wasm_threads: WASM_COMPILE_TIME_EH_FLAGS = -fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1

# Build + stage the coi (wasm_threads) variant. `wasm-all` is kept as an alias
# (coi is the only variant now).
.PHONY: wasm wasm-all
wasm wasm-all: wasm-preflight wasm_threads npm_db_wasm

# Regenerate TS + C++ mirrors of packages/python/src/n6k_protocol/protocol.py.
# Source of truth lives in Python; outputs are committed to git.
.PHONY: protocol-gen protocol-check
PROTOCOL_GENERATED := packages/npm/src/protocol-generated.ts src/common/include/n6k_protocol_generated.hpp packages/python/src/n6k_protocol/schema.json

protocol-gen:
	python -m n6k_protocol.codegen

# Verify the committed generated files match the source. Run this by hand after
# editing protocol.py -- no CI job or pre-commit hook invokes it.
protocol-check:
	python -m n6k_protocol.codegen
	@if ! git diff --quiet -- $(PROTOCOL_GENERATED); then \
		echo "ERROR: generated protocol files are out of sync with packages/python/src/n6k_protocol/protocol.py"; \
		echo "       run 'make protocol-gen' and commit the changes."; \
		git --no-pager diff -- $(PROTOCOL_GENERATED); \
		exit 1; \
	fi
	@echo "protocol mirrors are up to date"

