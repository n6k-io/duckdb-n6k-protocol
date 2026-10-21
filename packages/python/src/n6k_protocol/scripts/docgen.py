"""Expand ``:::`` placeholders in a markdown outline into signatures + docstrings.

Usage::

    n6k-docs <outline.md> <output.md>

The outline may begin with a frontmatter block::

    ---
    modules:
      _duckdb.DuckDBPyConnection: duckdb.DuckDBPyConnection
      pyarrow.lib.Table: pyarrow.Table
    ---

Each ``modules`` entry is a string replacement applied to rendered signatures —
internal module paths that leak out of ``inspect.signature`` get rewritten to
the names consumers actually import.

Each body line matching ``^\\s*::: dotted.path\\s*$`` is replaced with the
resolved object's signature (callables and classes) and docstring. Other
lines pass through unchanged.
"""

import importlib
import inspect
import re
import sys
from pathlib import Path
from typing import Any

_LINE = re.compile(r"^\s*:::\s+([\w\.]+)\s*$")
_FRONTMATTER = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)


def _parse_frontmatter(text: str) -> tuple[dict[str, str], str]:
    """Return (module_map, text_without_frontmatter)."""
    m = _FRONTMATTER.match(text)
    if not m:
        return {}, text
    body = m.group(1)
    rest = text[m.end() :]
    mapping: dict[str, str] = {}
    in_modules = False
    for line in body.splitlines():
        if not line.strip():
            continue
        if not line.startswith(" ") and line.rstrip().endswith(":"):
            in_modules = line.strip() == "modules:"
            continue
        if in_modules and ":" in line:
            k, _, v = line.strip().partition(":")
            mapping[k.strip()] = v.strip()
    return mapping, rest


def _resolve(path: str) -> Any:
    """Longest importable prefix, then ``getattr``-walk the rest."""
    parts = path.split(".")
    last_err: Exception | None = None
    for i in range(len(parts), 0, -1):
        try:
            obj: Any = importlib.import_module(".".join(parts[:i]))
        except ImportError as e:
            last_err = e
            continue
        try:
            for attr in parts[i:]:
                obj = getattr(obj, attr)
        except AttributeError as e:
            last_err = e
            continue
        return obj
    raise LookupError(f"cannot resolve {path!r}: {last_err}")


def _normalize(text: str, mapping: dict[str, str]) -> str:
    for old, new in mapping.items():
        text = text.replace(old, new)
    return text


def _split_top_level(s: str) -> list[str]:
    """Split on commas that are not inside brackets/parens."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(s):
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(s[start:i].strip())
            start = i + 1
    parts.append(s[start:].strip())
    return [p for p in parts if p]


def _format_sig(prefix: str, sig: inspect.Signature, mapping: dict[str, str], width: int = 80) -> str:
    """Render signature as `prefix(args) -> ret`; break multi-line when long."""
    single = _normalize(f"{prefix}{sig}", mapping)
    if len(single) <= width:
        return single
    sig_str = _normalize(str(sig), mapping)
    # str(sig) is like `(a: int, *, b: str = "x") -> None`. Split into args/return.
    ret = ""
    body = sig_str
    if body.endswith(":"):
        body = body[:-1]
    m = re.match(r"\((.*)\)(\s*->\s*.+)?$", body, re.DOTALL)
    if not m:
        return single
    args_str, ret = m.group(1), m.group(2) or ""
    args = _split_top_level(args_str)
    indented = ",\n    ".join(args)
    return f"{prefix}(\n    {indented},\n){ret}"


def _render(obj: Any, path: str, mapping: dict[str, str]) -> str:
    doc = inspect.getdoc(obj) or ""
    name = path.rsplit(".", 1)[-1]
    if inspect.ismodule(obj):
        return doc + "\n"
    if inspect.isclass(obj):
        try:
            sig = _format_sig(f"class {name}", inspect.signature(obj), mapping)
        except (ValueError, TypeError):
            sig = f"class {name}"
        body = f"```python\n{sig}\n```\n\n{doc}".rstrip()
        return body + "\n"
    if callable(obj):
        try:
            sig = _format_sig(name, inspect.signature(obj), mapping)
        except (ValueError, TypeError):
            sig = name
        body = f"```python\n{sig}\n```\n\n{doc}".rstrip()
        return body + "\n"
    return f"```python\n{name} = {obj!r}\n```\n"


def expand(text: str) -> str:
    mapping, rest = _parse_frontmatter(text)
    out: list[str] = []
    for line in rest.splitlines():
        m = _LINE.match(line)
        if m is None:
            out.append(line)
            continue
        path = m.group(1)
        out.append(_render(_resolve(path), path, mapping))
    return "\n".join(out) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        print("usage: n6k-docs <outline.md> <output.md>", file=sys.stderr)
        sys.exit(2)
    src, dst = Path(sys.argv[1]), Path(sys.argv[2])
    dst.write_text(expand(src.read_text()))
    print(f"{src} -> {dst}")


if __name__ == "__main__":
    main()
