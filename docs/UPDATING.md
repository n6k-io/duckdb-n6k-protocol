# Extension updating 
When cloning this template, the target version of DuckDB should be the latest stable release of DuckDB. However, there 
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release
  - `./extension-ci-tools` should be set to updated branch corresponding to latest DuckDB release. So if you're building for DuckDB `v1.1.0` there will be a branch in `extension-ci-tools` named `v1.1.0` to which you should check out. 
- Bump versions in `./.github/workflows/MainDistributionPipeline.yml`
  - the `duckdb_version` and `ci_tools_version` inputs of the `duckdb-stable-build` job
  - the same two inputs of the `code-quality-check` job
  - the `@ref` on both reusable workflows (`_extension_distribution.yml` and `_extension_code_quality.yml`)
  - the `git checkout <version>` in the `duckdb-stable-deploy` job, and the `n6k-<version>-extension-…` artifact name it downloads. The deploy job has no `duckdb_version` input — it derives `DUCKDB_VERSION` from the tag on the checked-out submodule.
- Bump the hardcoded `wasm/<version>/` paths and `n6k-<version>-extension-…` artifact name in `./.github/workflows/release-npm.yml`
- Bump the matching client pins so they agree with the extension build, or it won't load:
  - `duckdb==<version>` in the root `pyproject.toml` (then re-lock `uv.lock`)
  - `@duckdb/node-api` and `@duckdb/duckdb-wasm` in `packages/npm/package.json` and `test-app/package.json`. The duckdb-wasm build must bundle the same DuckDB core — check which core version a given `1.33.1-devN.0` carries via the `submodules/duckdb` bump history in `duckdb/duckdb-wasm`, since npm's `latest` tag can lag a DuckDB release.
  - `ARG DUCKDB_VERSION` in the root `Dockerfile`

# Vendored nanoarrow (`third_party/nanoarrow/`, currently 0.9.0)

The vendored copy is the unmodified output of nanoarrow's own bundler, with the
include paths flattened. To upgrade:

```
curl -sL -o na.tar.gz https://github.com/apache/arrow-nanoarrow/archive/refs/tags/apache-arrow-nanoarrow-<version>.tar.gz
tar xzf na.tar.gz
cd arrow-nanoarrow-apache-arrow-nanoarrow-<version>
python3 ci/scripts/bundle.py --symbol-namespace N6kArrow --with-ipc --with-flatcc --output-dir out
```

Then flatten into `third_party/nanoarrow/`:
- `out/src/*.c` and `out/include/nanoarrow/*` go to `third_party/nanoarrow/`
- `out/include/flatcc/` goes to `third_party/nanoarrow/flatcc/`
- rewrite `#include "nanoarrow/X.h"` to `#include "X.h"` in the copied
  `nanoarrow*.{h,c,hpp}` files:
  `sed -i '' 's|#include "nanoarrow/|#include "|' nanoarrow*.c nanoarrow*.h nanoarrow*.hpp`

No other local patches exist; the result must be byte-identical to the bundler
output apart from that include rewrite. `src/common/n6k_common.cmake` lists the
sources by name and feeds both the native and wasm builds, so no build-file
changes are needed unless the file set changes.

# API changes
DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:
- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed