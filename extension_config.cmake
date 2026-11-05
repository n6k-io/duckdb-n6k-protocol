# This file is included by DuckDB's build system. It specifies which extension to load

file(READ ${CMAKE_CURRENT_LIST_DIR}/VERSION N6K_VERSION)
string(STRIP "${N6K_VERSION}" N6K_VERSION)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${CMAKE_CURRENT_LIST_DIR}/VERSION)

# Extension from this repo
duckdb_extension_load(n6k_client
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/n6k_client/include
    EXTENSION_VERSION "v${N6K_VERSION}"
    LOAD_TESTS
)

# n6k_server serves over a Unix domain socket (src/n6k_server/uds_transport.cpp:
# AF_UNIX, sys/socket.h, poll, etc.), which has no Windows equivalent. The whole
# extension is POSIX-only, so don't build it on Windows. (WIN32 is true for both
# MSVC and the MinGW cross-toolchain.)
if(NOT WIN32)
    duckdb_extension_load(n6k_server
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/n6k_server
        INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/n6k_server/include
        EXTENSION_VERSION "v${N6K_VERSION}"
        LOAD_TESTS
    )
endif()

# n6k_testing holds the SQL functions that exist only to be called by tests. It is
# built so sqllogictest and pytest can `require`/LOAD it, and deliberately never
# published -- .github/workflows/MainDistributionPipeline.yml skips it in the
# upload loop.
#
# Native-only, for two reasons. Every caller is native (test/sql, pytest,
# integration_tests); nothing in packages/npm reaches it. And `make npm_db_wasm`
# copies build/wasm_threads/repository/* wholesale into packages/npm/wasm/, so a
# wasm build of this extension would ship a test-only binary inside the npm
# package. WASM_LOADABLE_EXTENSIONS is the flag the extension CMakeLists use;
# EMSCRIPTEN is checked too because this file is included before duckdb sets it.
if(NOT WIN32 AND NOT EMSCRIPTEN AND NOT WASM_LOADABLE_EXTENSIONS)
    duckdb_extension_load(n6k_testing
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/n6k_testing
        INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/n6k_testing/include
        EXTENSION_VERSION "v${N6K_VERSION}"
        LOAD_TESTS
    )
    # v2 links only extensions listed here into the shell; integration tests spawn it expecting these built in.
    duckdb_extension_statically_link(n6k_client n6k_server n6k_testing)
endif()