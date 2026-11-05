cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME n6k_testing)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Before the targets exist: see the root CMakeLists.txt.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# What this extension exercises lives in src/common, so this target never
# reaches into another extension's private include directory.
include(${CMAKE_CURRENT_SOURCE_DIR}/../common/n6k_common.cmake)
n6k_common_includes()

# The code under test. Compiled in rather than linked: extensions here share
# code by recompiling the same .cpp into each target, so these must be the same
# sources the shipped extensions build, not a copy.
n6k_common_sources(EXTENSION_SOURCES n6k_sql_builder serve_bind_common
                   n6k_arrow_frame_decoder arrow_ipc_encode)
n6k_nanoarrow_sources(EXTENSION_SOURCES)

list(
  APPEND
  EXTENSION_SOURCES
  n6k_testing_extension.cpp
  sql_builder_functions.cpp
  fixture_functions.cpp
  arrow_ipc_decode_selftest.cpp
  wire_codec_bench.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# yyjson (vendored in duckdb) for parsing the JSON arguments the builder scalars
# take, and for the bodies n6k_sql_builder.cpp renders.
target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
