#!/usr/bin/env python3
"""Extract Cloudscape design-token *values* into a plain CSS custom-properties
stylesheet, so the admin Lambda can use the Cloudscape visual language with no
runtime dependency on @cloudscape-design/design-tokens and no React.

Input:  the blessed `index-visual-refresh.json` from
        @cloudscape-design/design-tokens (the token VALUES artifact).
Output: a `:root { --<token>: <value>; }` stylesheet on stdout.

Each token's `$value` is one of:
  - a string (font-size, border-radius, ...)              -> used verbatim
  - {light, dark}        (colors, shadows)                -> light value
  - {comfortable, compact} (space, size)                  -> comfortable value
  - {default, disabled}  (motion)                         -> default value

The choice of variant is fixed (light / comfortable / default) so the output is
deterministic. Token names map to `--<name>` custom properties, matching the
`$name` convention Cloudscape itself uses.

Usage:
    extract_tokens.py path/to/index-visual-refresh.json > tokens.css
"""

from __future__ import annotations

import json
import sys


def resolve(value: object) -> str | None:
    """Reduce a token $value to a single CSS value string, or None to skip."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("light", "comfortable", "default"):
            if key in value:
                return value[key]
    return None


def to_css(tokens: dict) -> str:
    lines = [
        "/* Generated from @cloudscape-design/design-tokens index-visual-refresh.json.",
        " * Do not edit by hand; regenerate with scripts/extract_tokens.py. */",
        ":root {",
    ]
    for name in sorted(tokens):
        resolved = resolve(tokens[name].get("$value"))
        if resolved is not None:
            lines.append(f"  --{name}: {resolved};")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: extract_tokens.py <index-visual-refresh.json>", file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as f:
        data = json.load(f)
    sys.stdout.write(to_css(data.get("tokens", {})))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
