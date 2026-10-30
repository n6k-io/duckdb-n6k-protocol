cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME n6k_testing)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Pin the C++ standard before build_*_extension create the targets (the standard
# is captured into each target's CXX_STANDARD property at creation time, so
# setting it afterwards is a no-op). _REQUIRED ON makes an unsupported standard
# a hard error instead of silently decaying to an older one. Pinned at 14 so a
# local build fails the same way CI's pre-C++17 toolchain would, catching
# accidental C++17 usage early.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# src/common/n6k_common.cmake is the single declaration of what `src/common/`
# holds and how to consume it. Everything this extension exercises —
# n6k_sql_builder, serve_bind_common's ResolveServedCatalogs — lives there, so
# this target never reaches into another extension's private include directory.
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
