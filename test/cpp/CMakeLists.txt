cmake_minimum_required(VERSION 3.5)

if(NOT DUCKDB_SOURCE_DIR OR NOT DUCKDB_BUILD_DIR)
  message(FATAL_ERROR "set DUCKDB_SOURCE_DIR and DUCKDB_BUILD_DIR")
endif()

load_cache(
  ${DUCKDB_BUILD_DIR}
  READ_WITH_PREFIX N6K_RELEASE_ CMAKE_OSX_DEPLOYMENT_TARGET OPENSSL_SSL_LIBRARY
                   OPENSSL_CRYPTO_LIBRARY OPENSSL_INCLUDE_DIR)
if(N6K_RELEASE_CMAKE_OSX_DEPLOYMENT_TARGET)
  set(CMAKE_OSX_DEPLOYMENT_TARGET ${N6K_RELEASE_CMAKE_OSX_DEPLOYMENT_TARGET})
endif()

project(n6k_unittest C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(N6K_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." REALPATH)

include(${N6K_ROOT}/src/common/n6k_common.cmake)
include(${N6K_ROOT}/third_party/crossing/crossing.cmake)
n6k_common_includes()
crossing_substrait_includes(CROSSING_INCLUDES)

set(N6K_ARCHIVES
    ${DUCKDB_BUILD_DIR}/extension/n6k_client/libn6k_client_extension.a
    ${DUCKDB_BUILD_DIR}/extension/n6k_server/libn6k_server_extension.a
    ${DUCKDB_BUILD_DIR}/extension/n6k_testing/libn6k_testing_extension.a
    ${DUCKDB_BUILD_DIR}/extension/core_functions/libcore_functions_extension.a
    ${DUCKDB_BUILD_DIR}/src/main/http/libduckdb_httplib.a
    ${DUCKDB_BUILD_DIR}/third_party/utf8proc/libduckdb_utf8proc.a
    ${DUCKDB_BUILD_DIR}/third_party/yyjson/libduckdb_yyjson.a
    ${DUCKDB_BUILD_DIR}/third_party/zstd/libduckdb_zstd.a
    ${DUCKDB_BUILD_DIR}/src/libduckdb_static.a
    ${N6K_RELEASE_OPENSSL_SSL_LIBRARY}
    ${N6K_RELEASE_OPENSSL_CRYPTO_LIBRARY})

foreach(lib ${N6K_ARCHIVES})
  if(NOT EXISTS ${lib})
    message(FATAL_ERROR "missing ${lib} -- run `make release` once")
  endif()
endforeach()

find_package(Threads REQUIRED)

set(LINK_EXTENSION_LIST core_functions)
set(DESCRIBE_DECLARATIONS
    "int32_t duckdb_extension_core_functions_describe(duckdb_extension_descriptor *descriptor);"
)
set(DESCRIBE_REGISTRATIONS
    "\tif (duckdb_register_static_extension(duckdb_extension_core_functions_describe) != 0) {\n\t\tresult = 1;\n\t}"
)
set(N6K_EXTENSION_LOADER
    ${CMAKE_CURRENT_BINARY_DIR}/n6k_static_extension_loader.c)
configure_file(
  ${DUCKDB_SOURCE_DIR}/extension/loader/static_extension_loader.c.in
  ${N6K_EXTENSION_LOADER} @ONLY)

file(GLOB N6K_TEST_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp)

add_executable(
  n6k_unittest
  unittest_main.cpp ${N6K_EXTENSION_LOADER}
  ${DUCKDB_SOURCE_DIR}/extension/loader/static_extension_autoregister.cpp
  ${N6K_TEST_SOURCES})

target_include_directories(
  n6k_unittest
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
          ${N6K_ROOT}/src/n6k_client/include
          ${N6K_ROOT}/src/n6k_server/include
          ${N6K_ROOT}/src/n6k_testing/include
          ${N6K_ROOT}/third_party/ixwebsockets
          ${CROSSING_INCLUDES}
          ${DUCKDB_SOURCE_DIR}/src/include
          ${DUCKDB_SOURCE_DIR}/third_party/catch
          ${DUCKDB_SOURCE_DIR}/third_party/fmt/include
          ${DUCKDB_SOURCE_DIR}/third_party/yyjson/include
          ${N6K_RELEASE_OPENSSL_INCLUDE_DIR})

if(APPLE)
  target_link_libraries(n6k_unittest ${N6K_ARCHIVES} Threads::Threads)
else()
  target_link_libraries(n6k_unittest -Wl,--start-group ${N6K_ARCHIVES}
                        -Wl,--end-group Threads::Threads ${CMAKE_DL_LIBS})
endif()
