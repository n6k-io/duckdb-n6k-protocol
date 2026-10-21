# IXWebSocket (vendored)

Upstream: https://github.com/machinezone/IXWebSocket
Version: v11.4.6
License: BSD-3-Clause (see LICENSE.txt)

Used by `native_transport.cpp` for the native-build WebSocket session client.

## Build configuration

Compiled with `IXWEBSOCKET_USE_TLS` + `IXWEBSOCKET_USE_OPEN_SSL`. The Apple
Secure Transport and mbedTLS backends are present but inert (each is guarded
by its own `IXWEBSOCKET_USE_*` define). Zlib / PerMessageDeflate is disabled.

## Upgrading

1. Download the tagged release tarball from upstream.
2. Replace `ixwebsocket/*.{h,cpp}` with the new contents.
3. Update the version string above and bump `third_party/ixwebsockets/CMakeLists.txt`
   if upstream adds or removes source files.
