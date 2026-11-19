#!/usr/bin/env bash
# Checks that a build.zig.zon pins a core bundle for each target: a real entry under
# .dependencies named core-<core version>-<target>, with a .url that names that target's
# bundle and a non-empty .hash. It reads the manifest's structure, not its text, so a pin
# inside a comment or under another key doesn't count. It can't tell whether a hash matches
# the published bundle: a build that fetches the pin checks that.
#
# Usage: scripts/core-pins.sh <build.zig.zon> <core version> <zig target>...
# Prints one line per missing or broken pin, naming it, and exits 1 if there is any.
set -euo pipefail
if [ $# -lt 3 ]; then
  echo "usage: $0 <build.zig.zon> <core version> <zig target>..." >&2
  exit 2
fi
python3 -I - "$@" <<'EOF'
import re, sys

zon_path, core_version, *targets = sys.argv[1:]
text = open(zon_path, encoding="utf-8").read()

# A small reader for the ZON a manifest uses: .{ } structs and tuples, .name and .@"name"
# fields, strings, numbers, enum literals and true/false/null. Comments run from // to the
# end of the line, and are skipped like whitespace.
token = re.compile(r'''
    (?P<space>\s+|//[^\n]*)
  | (?P<string>"(?:[^"\\\n]|\\.)*")
  | (?P<quoted>\.@"(?:[^"\\\n]|\\.)*")
  | (?P<open>\.\{)
  | (?P<field>\.[A-Za-z_][A-Za-z0-9_]*)
  | (?P<word>[A-Za-z0-9_][A-Za-z0-9_.+-]*)
  | (?P<punct>[{}=,])
''', re.VERBOSE)
tokens, at = [], 0
while at < len(text):
    found = token.match(text, at)
    if not found:
        sys.exit(f"{zon_path}: can't read the manifest at offset {at}: {text[at:at + 20]!r}")
    if found.lastgroup != "space":
        tokens.append((found.lastgroup, found.group()))
    at = found.end()

def unquote(literal):
    return bytes(literal[1:-1], "utf-8").decode("unicode_escape")

def value(i):
    if i >= len(tokens):
        sys.exit(f"{zon_path}: the manifest ends where a value should be")
    kind, text = tokens[i]
    if kind == "string":
        return unquote(text), i + 1
    if kind == "open":
        return container(i + 1)
    if kind in ("field", "word"):
        return text, i + 1
    sys.exit(f"{zon_path}: unexpected {text!r}")

def container(i):
    fields, items = {}, []
    while True:
        if i >= len(tokens):
            sys.exit(f"{zon_path}: a .{{ }} struct is never closed")
        if tokens[i][1] == "}":
            break
        kind, text = tokens[i]
        if kind in ("field", "quoted") and i + 1 < len(tokens) and tokens[i + 1][1] == "=":
            name = text[1:] if kind == "field" else unquote(text[2:])
            fields[name], i = value(i + 2)
        else:
            item, i = value(i)
            items.append(item)
        if i >= len(tokens):
            sys.exit(f"{zon_path}: a .{{ }} struct is never closed")
        if tokens[i][1] == ",":
            i += 1
        elif tokens[i][1] != "}":
            sys.exit(f"{zon_path}: expected , or }} but found {tokens[i][1]!r}")
    return (fields if fields or not items else items), i + 1

if not tokens or tokens[0][0] != "open":
    sys.exit(f"{zon_path}: the manifest isn't a .{{ }} struct")
root, _ = container(1)
dependencies = root.get("dependencies") if isinstance(root, dict) else None
if not isinstance(dependencies, dict):
    dependencies = {}

problems = []
for target in targets:
    name = f"core-{core_version}-{target}"
    pin = dependencies.get(name)
    if not isinstance(pin, dict):
        problems.append(f"{name}: not an entry under .dependencies")
        continue
    url, digest = pin.get("url"), pin.get("hash")
    bundle = f"temporal-core-{core_version}-{target}.tar.xz"
    if not isinstance(url, str) or not url.endswith("/" + bundle):
        problems.append(f"{name}: its .url doesn't point at {bundle}")
    if not isinstance(digest, str) or not digest:
        problems.append(f"{name}: it has no .hash")
for problem in problems:
    print(problem)
sys.exit(1 if problems else 0)
EOF
