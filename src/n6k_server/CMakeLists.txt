cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME n6k_server)

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
# holds and how to consume it (include paths, module list, vendored nanoarrow),
# so no extension re-spells them.
include(${CMAKE_CURRENT_SOURCE_DIR}/../common/n6k_common.cmake)
n6k_common_includes()

include(${N6K_THIRD_PARTY}/crossing/crossing.cmake)
crossing_substrait_decode_sources(CROSSING_SUBSTRAIT_SOURCES)
crossing_execute_sources(CROSSING_EXECUTE_SOURCES)
crossing_substrait_includes(CROSSING_INCLUDES)
include_directories(${CROSSING_INCLUDES})

# Phase 6 — OP_TABLES_LIST via the shared plain-catalog permission collector.
n6k_common_sources(
  EXTENSION_SOURCES
  serve_bind_common
  arrow_ipc_encode
  n6k_permissions
  n6k_sql_builder
  n6k_catalog_list
  n6k_exception_types
  n6k_served
  n6k_fd_hello)
n6k_nanoarrow_sources(EXTENSION_SOURCES)
list(APPEND EXTENSION_SOURCES ${CROSSING_SUBSTRAIT_SOURCES}
     ${CROSSING_EXECUTE_SOURCES})

list(
  APPEND
  EXTENSION_SOURCES
  n6k_server_extension.cpp
  n6k_serve_function.cpp
  frame_codec.cpp
  uds_transport.cpp
  serve_handshake.cpp
  serve_options.cpp
  host_callbacks.cpp
  serve_reactor.cpp
  request_handlers.cpp
  insert_scan.cpp)

# n6k_serve_http listens with ixwebsocket's server. ixwebsocket is an OBJECT
# library defined by the ROOT CMakeLists (the n6k extension's) and only when NOT
# WASM_LOADABLE_EXTENSIONS; add it here only if that hasn't happened yet, so
# extension ordering doesn't matter. A wasm build keeps only the socket form.
if(NOT WASM_LOADABLE_EXTENSIONS)
  # The vendored ixwebsocket is compiled with IXWEBSOCKET_USE_TLS/_OPEN_SSL, so
  # its objects reference OpenSSL even though this server only speaks plain
  # ws://.
  find_package(OpenSSL REQUIRED)
  if(NOT TARGET ixwebsocket)
    add_subdirectory(${N6K_THIRD_PARTY}/ixwebsockets ixwebsockets_build)
  endif()
  # $<TARGET_OBJECTS:> does not propagate the target's PUBLIC include dirs, so
  # name the include path here (the root CMakeLists does the same for the n6k
  # extension).
  include_directories(${N6K_THIRD_PARTY}/ixwebsockets)
  add_definitions(-DN6K_SERVER_WITH_WS)
  list(APPEND EXTENSION_SOURCES n6k_serve_http_function.cpp
       ws_serve_transport.cpp ws_serve_connection.cpp
       $<TARGET_OBJECTS:ixwebsocket>)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# yyjson (vendored in duckdb) for building/parsing the small JSON frame bodies.
target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

if(NOT WASM_LOADABLE_EXTENSIONS)
  target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
