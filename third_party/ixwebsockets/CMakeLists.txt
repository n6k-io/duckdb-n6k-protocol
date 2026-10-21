# Vendored IXWebSocket (v11.4.6).
# Native-only — WASM builds must skip this subdirectory entirely.

if(WASM_LOADABLE_EXTENSIONS)
  message(FATAL_ERROR "third_party/ixwebsockets must not be added for WASM builds")
endif()

file(GLOB IXWEBSOCKET_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/ixwebsocket/*.cpp")

add_library(ixwebsocket OBJECT ${IXWEBSOCKET_SOURCES})

target_include_directories(ixwebsocket PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}"
  ${OPENSSL_INCLUDE_DIR})

target_compile_definitions(ixwebsocket PUBLIC
  IXWEBSOCKET_USE_TLS
  IXWEBSOCKET_USE_OPEN_SSL)

set_target_properties(ixwebsocket PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  CXX_STANDARD 14
  CXX_STANDARD_REQUIRED ON)
