cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME n6k_client)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Pin the C++ standard before build_*_extension create the targets (the standard
# is captured into each target's CXX_STANDARD property at creation time, so setting
# it afterwards is a no-op). _REQUIRED ON makes an unsupported standard a hard error
# instead of silently decaying to an older one. Pinned at 14 so a local build fails
# the same way CI's pre-C++17 toolchain would, catching accidental C++17 usage early.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# src/common/n6k_common.cmake is the single declaration of what `src/common/` holds and how to
# consume it; every extension includes it rather than re-spelling the paths.
include(${CMAKE_CURRENT_SOURCE_DIR}/src/common/n6k_common.cmake)
n6k_common_includes()

include(${CMAKE_CURRENT_SOURCE_DIR}/third_party/crossing/crossing.cmake)
crossing_sources(CROSSING_SOURCES)
crossing_substrait_render_sources(CROSSING_SUBSTRAIT_SOURCES)
crossing_substrait_includes(CROSSING_INCLUDES)
include_directories(${CROSSING_INCLUDES})

include_directories(src/n6k_client/include)

if(NOT WASM_LOADABLE_EXTENSIONS)
  find_package(OpenSSL REQUIRED)
  add_subdirectory(third_party/ixwebsockets)
  include_directories(third_party/ixwebsockets)
endif()

n6k_common_sources(EXTENSION_SOURCES n6k_permissions n6k_describe n6k_catalog_list
                   n6k_exception_types n6k_arrow_frame_decoder n6k_served n6k_fd_hello)
n6k_nanoarrow_sources(EXTENSION_SOURCES)
list(APPEND EXTENSION_SOURCES ${CROSSING_SOURCES} ${CROSSING_SUBSTRAIT_SOURCES})

list(
  APPEND
  EXTENSION_SOURCES
    src/n6k_client/n6k_client_extension.cpp
    src/n6k_client/n6k_table_function.cpp
    src/n6k_client/n6k_table_permissions.cpp
    src/n6k_client/n6k_storage.cpp
    src/n6k_client/n6k_catalog.cpp
    src/n6k_client/n6k_schema_entry.cpp
    src/n6k_client/n6k_source.cpp
    src/n6k_client/n6k_transaction.cpp
    src/n6k_client/n6k_insert.cpp
    src/n6k_client/n6k_rpc_function.cpp
    src/n6k_client/n6k_version_function.cpp
    src/n6k_client/n6k_capabilities_function.cpp
    src/n6k_client/n6k_thread_selftest.cpp
    src/n6k_client/n6k_io_thread_selftest.cpp
    src/n6k_client/ws_send_after_start_selftest.cpp
    src/n6k_client/open_ns_selftest.cpp
    src/n6k_client/open_pending_selftest.cpp
    src/n6k_client/ws_fd_hub_selftest.cpp
    src/n6k_client/n6k_connect_function.cpp
    src/n6k_client/n6k_list_catalogs_function.cpp
    src/n6k_client/n6k_parse_sql_get_tables.cpp
    src/n6k_client/n6k_split_statements.cpp
    src/n6k_client/n6k_secret.cpp
    src/n6k_client/n6k_credentials.cpp
    src/n6k_client/n6k_login_function.cpp
    src/n6k_client/n6k_str_utils.cpp)

# Shared client + session + scan across native and wasm. The catalog session and
# Arrow stream are transport-agnostic (they drive a WsClient/RequestState).
list(APPEND EXTENSION_SOURCES src/n6k_client/ws_client.cpp)
list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_catalog_session_native.cpp)
list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_ws_arrow_stream.cpp)

if(WASM_LOADABLE_EXTENSIONS)
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_http_client_wasm.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_reactor_wasm.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_io_thread.cpp)
else()
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_http_client_native.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_discovery.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_reactor_native.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_transport_ix.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_transport_fd.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_fd_hub.cpp)
  list(APPEND EXTENSION_SOURCES $<TARGET_OBJECTS:ixwebsocket>)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

if(WASM_LOADABLE_EXTENSIONS)
  target_compile_definitions(${EXTENSION_NAME} PRIVATE WASM_LOADABLE_EXTENSIONS=1)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE WASM_LOADABLE_EXTENSIONS=1)
else()
  # ixwebsocket is an OBJECT library whose .o files are compiled into both
  # n6k extension targets via $<TARGET_OBJECTS:ixwebsocket> above. OpenSSL
  # must still be linked by each consumer.
  target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
endif()


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
