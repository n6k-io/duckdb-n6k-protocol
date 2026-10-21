# The one declaration of what `src/common/` contains and how to consume it.
#
# These extensions share code by compiling the same .cpp into each target rather than by linking a
# library: `build_static_extension` and `build_loadable_extension` create two targets per extension
# with different flags, so a single set of object files cannot serve both. That constraint is real,
# but it does not require four hand-maintained copies of the same relative paths — which is what this
# file replaces. A module named here that has no matching source is a configure-time error, so a
# rename cannot leave one extension building a file the others no longer have.
#
# Include with:
#   include(${CMAKE_CURRENT_SOURCE_DIR}/../common/n6k_common.cmake)
# from an extension's CMakeLists, or the repo-root path from the root one.

get_filename_component(N6K_COMMON_DIR "${CMAKE_CURRENT_LIST_DIR}" REALPATH)
get_filename_component(N6K_REPO_ROOT "${N6K_COMMON_DIR}/../.." REALPATH)
# Canonical repo-root path, with no `src/` component. The tidy runner's
# `-header-filter '<proj>/src/.*/'` matches any header whose path contains `/src/`, so a
# `src/<ext>/../../third_party/...` spelling would drag these vendored headers into the analysis
# (hundreds of false positives). Resolving here means no extension can spell it the other way.
set(N6K_THIRD_PARTY "${N6K_REPO_ROOT}/third_party")

# Header search paths every extension needs to compile common sources. msgpack-cxx is header-only;
# -DMSGPACK_NO_BOOST keeps it dependency-free and C++14-clean.
macro(n6k_common_includes)
  include_directories(${N6K_COMMON_DIR}/include)
  include_directories(${N6K_THIRD_PARTY}/nanoarrow)
  include_directories(${N6K_THIRD_PARTY}/msgpack/include)
  add_definitions(-DMSGPACK_NO_BOOST)
endmacro()

# Append the named common modules to `out_var`. Each name is a bare stem: `n6k_permissions`
# resolves to `src/common/n6k_permissions.cpp`. Unknown names are fatal rather than silently
# dropped — a typo here would otherwise surface as a link error in one extension only.
function(n6k_common_sources out_var)
  set(resolved ${${out_var}})
  foreach(module ${ARGN})
    set(path "${N6K_COMMON_DIR}/${module}.cpp")
    if(NOT EXISTS "${path}")
      message(FATAL_ERROR "n6k_common_sources: no such module '${module}' (looked for ${path})")
    endif()
    list(APPEND resolved "${path}")
  endforeach()
  set(${out_var} ${resolved} PARENT_SCOPE)
endfunction()

# The vendored nanoarrow C sources, compiled into any extension that encodes or decodes Arrow IPC.
function(n6k_nanoarrow_sources out_var)
  set(${out_var}
      ${${out_var}}
      ${N6K_THIRD_PARTY}/nanoarrow/nanoarrow.c
      ${N6K_THIRD_PARTY}/nanoarrow/nanoarrow_ipc.c
      ${N6K_THIRD_PARTY}/nanoarrow/flatcc.c
      PARENT_SCOPE)
endfunction()
