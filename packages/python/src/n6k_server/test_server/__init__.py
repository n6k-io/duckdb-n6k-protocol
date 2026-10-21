"""n6k protocol test server.

Importing the package pins the bridge extension to the local ``build/release``
build (see ``_paths.pin_local_extension``) before any handler seeds a
bridge-backed catalog, so the test server can never silently load a stale
cached extension.
"""

from n6k_server.test_server._paths import pin_local_extension

pin_local_extension()
