cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME n6k_client)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Before build_*_extension create the targets: the standard is captured into
# each target's CXX_STANDARD property at creation time, so setting it afterwards
# is a no-op.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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

n6k_common_sources(
  EXTENSION_SOURCES
  n6k_permissions
  n6k_describe
  n6k_catalog_list
  n6k_exception_types
  n6k_arrow_frame_decoder
  n6k_served
  n6k_host_callback
  n6k_ws_log)
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
  src/n6k_client/n6k_rpc_function.cpp
  src/n6k_client/n6k_version_function.cpp
  src/n6k_client/n6k_capabilities_function.cpp
  src/n6k_client/n6k_client_settings_function.cpp
  src/n6k_client/n6k_thread_selftest.cpp
  src/n6k_client/n6k_io_thread_selftest.cpp
  src/n6k_client/ws_send_after_start_selftest.cpp
  src/n6k_client/open_ns_selftest.cpp
  src/n6k_client/open_pending_selftest.cpp
  src/n6k_client/connection_hub_selftest.cpp
  src/n6k_client/n6k_connect_function.cpp
  src/n6k_client/n6k_connection_functions.cpp
  src/n6k_client/n6k_list_catalogs_function.cpp
  src/n6k_client/n6k_parse_sql_get_tables.cpp
  src/n6k_client/n6k_split_statements.cpp
  src/n6k_client/n6k_secret.cpp
  src/n6k_client/n6k_credentials.cpp
  src/n6k_client/n6k_login_function.cpp
  src/n6k_client/n6k_str_utils.cpp)

# Shared client + session + scan across native and wasm. The catalog session and
# Arrow stream are transport-agnostic (they drive a WsClient/RequestState).
list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_endpoint.cpp)
list(APPEND EXTENSION_SOURCES src/n6k_client/ws_client.cpp)
list(APPEND EXTENSION_SOURCES src/n6k_client/connection_hub.cpp)
list(APPEND EXTENSION_SOURCES src/n6k_client/hub_session.cpp)
list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_catalog_session.cpp)
list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_ws_arrow_stream.cpp)

if(WASM_LOADABLE_EXTENSIONS)
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_http_client_wasm.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_transport_vsock.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_io_thread.cpp)
else()
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_http_client_native.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/n6k_discovery.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_transport_ix.cpp)
  list(APPEND EXTENSION_SOURCES src/n6k_client/ws_transport_raw.cpp)
  list(APPEND EXTENSION_SOURCES $<TARGET_OBJECTS:ixwebsocket>)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

if(WASM_LOADABLE_EXTENSIONS)
  target_compile_definitions(${EXTENSION_NAME}
                             PRIVATE WASM_LOADABLE_EXTENSIONS=1)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME}
                             PRIVATE WASM_LOADABLE_EXTENSIONS=1)
else()
  # $<TARGET_OBJECTS:ixwebsocket> above carries no link dependencies, so each of
  # these targets links OpenSSL itself.
  target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
