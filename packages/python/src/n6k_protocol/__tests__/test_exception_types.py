"""The `exception_type` SSOT, and the three places it has to stay in step with.

`exception_type` is a closed set: the client reconstructs a DuckDB exception class
from it and treats anything it does not recognize as `IOException`. That fallback is
silent, so a wrong value does not fail — it just quietly strips the type off every
error. These tests are the fence around that.

The set previously lived in three hand-maintained places and had drifted;
`DependencyException` was documented and reachable but unmapped, so it went out as
`IOException`.
"""

import re

from n6k_protocol.protocol import EXCEPTION_TYPES

# Repo root from packages/python/src/n6k_protocol/__tests__/
import os

_REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))


def test_every_type_is_a_class_name() -> None:
    """The wire value is the DuckDB class name, which is what the client matches on
    (`N6kExceptionTypeFromClassName`). A display string like "Catalog" would be
    silently downgraded."""
    for name in EXCEPTION_TYPES:
        assert name.endswith("Exception"), f"{name} is not a DuckDB class name"


def test_no_duplicates() -> None:
    assert len(EXCEPTION_TYPES) == len(set(EXCEPTION_TYPES))


def test_docs_table_matches_the_ssot() -> None:
    """The protocol doc enumerates the same set in prose. It is the spec an
    independent implementer reads, so a value here that is missing there is
    undocumented, and one there that is missing here cannot actually be produced."""
    doc = os.path.join(_REPO_ROOT, "docs", "n6k-network-protocol.md")
    with open(doc, encoding="utf-8") as f:
        text = f.read()

    section = text.split("`exception_type` is the name of the DuckDB")[1]
    section = section.split("Unknown values MUST")[0]
    documented = set(re.findall(r"`(\w+Exception)`", section))

    assert documented == set(EXCEPTION_TYPES), (
        f"docs/n6k-network-protocol.md and protocol.py disagree; "
        f"documented-only={sorted(documented - set(EXCEPTION_TYPES))}, "
        f"ssot-only={sorted(set(EXCEPTION_TYPES) - documented)}"
    )


def test_cpp_wire_map_matches_the_ssot() -> None:
    """`src/common/n6k_exception_types.cpp` is the C++ half of the same set. It is
    what the native server sends and what a non-C++ host reads back through
    `n6k_exception_types()`, so a value it can emit that is not in the SSOT is a value
    the client will silently downgrade."""
    src = os.path.join(_REPO_ROOT, "src", "common", "n6k_exception_types.cpp")
    with open(src, encoding="utf-8") as f:
        text = f.read()

    table = text.split("WIRE_EXCEPTION_MAP[] = {")[1].split("};")[0]
    in_cpp = set(re.findall(r'"(\w+Exception)"', table))

    assert in_cpp == set(EXCEPTION_TYPES), (
        f"src/common/n6k_exception_types.cpp and protocol.py disagree; "
        f"cpp-only={sorted(in_cpp - set(EXCEPTION_TYPES))}, "
        f"ssot-only={sorted(set(EXCEPTION_TYPES) - in_cpp)}"
    )
