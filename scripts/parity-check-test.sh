#!/bin/sh
# Tests scripts/parity-check.sh on throwaway copies of a small PARITY.md, README.md and
# samples directory, so it changes nothing in this checkout. Prints one line per failed case
# and exits 1 if any failed.
#
# Usage: scripts/parity-check-test.sh
set -eu
src="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
failures=0
cases=0
# The fixture's official SDKs, placeholder names: parity-check.sh reads them from these links
# in PARITY.md, and only PARITY.md names real SDKs.
sdk_links='[Alpha](https://github.com/temporalio/sdk-alpha), [Beta](https://github.com/temporalio/sdk-beta), [Gamma](https://github.com/temporalio/sdk-gamma)'

# A repository whose README.md repeats a heading, has a heading with punctuation, and has a
# heading inside a code block, with one sample.
new_repo() {
  repo="$tmp/repo$cases"
  mkdir -p "$repo/samples/hello"
  : > "$repo/samples/hello/worker.zig"
  : > "$repo/samples/hello/sample.zon"
  : > "$repo/samples/dispatch.zig"
  cat > "$repo/README.md" <<'EOF'
# Title

## Signals, queries, and updates

### Definition

```sh
# Not a heading
```

## Workflows

### Definition

### `ctx.fail` and failures
EOF
}

# parity <rows...>: writes PARITY.md with a feature table of the given rows, then other sections.
parity() {
  {
    printf '# Parity\n\n## How parity is measured\n\n%s\n\n## Feature table\n\n### Area\n\n' "$sdk_links"
    printf '| Feature | SDKs | Stage | Zig | Where |\n|---------|------|-------|-----|-------|\n'
    for row in "$@"; do printf '%s\n' "$row"; done
    printf '\n## Samples\n\n| Zig sample | Other |\n|---|---|\n| [nope](samples/nope) | nope |\n'
  } > "$repo/PARITY.md"
}

# expect <pass|fail> <name> <text the output must contain> <rows...>
expect() {
  outcome="$1" name="$2" want="$3"
  shift 3
  cases=$((cases + 1))
  new_repo
  parity "$@"
  status=0
  out="$(sh "$src/scripts/parity-check.sh" "$repo" 2>&1)" || status=$?
  if [ "$outcome" = pass ] && [ "$status" -ne 0 ]; then
    echo "FAIL $name: expected success, got exit $status: $out"
    failures=$((failures + 1))
  elif [ "$outcome" = fail ] && [ "$status" -eq 0 ]; then
    echo "FAIL $name: expected failure, got success: $out"
    failures=$((failures + 1))
  elif [ -n "$want" ] && ! printf '%s\n' "$out" | grep -qF -- "$want"; then
    echo "FAIL $name: output lacks \"$want\": $out"
    failures=$((failures + 1))
  fi
}

# The backticks are a Markdown code span, not a command substitution.
# shellcheck disable=SC2016
expect pass "every status, links, and the GA gap list" "partial  Partly (temporal-sdk-zig-d51.1)" \
  '| Linked | all | GA | supported | [S](README.md#signals-queries-and-updates), [h](samples/hello) |' \
  '| Repeated heading | all | GA | supported | [D](README.md#definition-1) |' \
  '| Punctuated heading | all | GA | supported | [F](README.md#ctxfail-and-failures) |' \
  '| Partly | Alpha, Beta | GA | partial | `temporal-sdk-zig-d51.1`. Missing: the rest. |' \
  '| Not yet | Alpha, Beta | pre-release | missing | unfiled |' \
  '| Elsewhere | Alpha, Beta | GA | not applicable | Zig has no such thing. |'
expect pass "a GA missing row is printed" "missing  Absent (unfiled)" \
  '| Absent | Alpha, Beta | GA | missing | unfiled |'
expect pass "pre-release gaps are not printed" "Every GA feature is supported" \
  '| Later | Alpha, Beta | pre-release | missing | unfiled |'
expect fail "unknown status" 'unknown Zig status "done"' \
  '| Feature | Alpha, Beta | GA | done | [S](README.md#workflows) |'
expect fail "unknown stage" 'unknown stage "beta"' \
  '| Feature | Alpha, Beta | beta | supported | [S](README.md#workflows) |'
expect fail "supported without a link" "links a README.md section or a sample" \
  '| Feature | Alpha, Beta | GA | supported | somewhere |'
expect fail "README anchor that doesn't exist" "no heading for #definition-2" \
  '| Feature | Alpha, Beta | GA | supported | [D](README.md#definition-2) |'
expect fail "heading inside a code block is no anchor" "no heading for #not-a-heading" \
  '| Feature | Alpha, Beta | GA | supported | [D](README.md#not-a-heading) |'
expect fail "sample that doesn't exist" "no sample directory samples/missing" \
  '| Feature | Alpha, Beta | GA | supported | [m](samples/missing) |'
expect fail "missing without a bead or unfiled" 'names its bead ID or "unfiled"' \
  '| Feature | Alpha, Beta | GA | missing | later |'
expect fail "partial without a bead or unfiled" 'names its bead ID or "unfiled"' \
  '| Feature | Alpha, Beta | GA | partial | Missing: half. |'
expect fail "partial that doesn't say what is missing" "says what is missing" \
  '| Feature | Alpha, Beta | GA | partial | unfiled |'
expect fail "not applicable without a reason" "gives the reason" \
  '| Feature | Alpha, Beta | GA | not applicable | |'
expect fail "wrong column count" "needs 5 columns" \
  '| Feature | Alpha, Beta | GA | supported |'
expect fail "empty table" "no rows"

# GitHub renders a table row indented by up to three spaces, or one without the leading pipe,
# as a row of the table, so the check must read those rows too.
expect fail "an indented row with an unknown status" 'unknown Zig status "done"' \
  '| Linked | all | GA | supported | [S](README.md#workflows) |' \
  '  | Indented | Alpha, Beta | GA | done | [S](README.md#workflows) |'
expect fail "a row without the leading pipe with an unknown status" 'unknown Zig status "done"' \
  '| Linked | all | GA | supported | [S](README.md#workflows) |' \
  'No pipe | Alpha, Beta | GA | done | [S](README.md#workflows) |'

# expect_readme <pass|fail> <name> <text the output must contain> <README.md> <rows...>: as
# expect, with the given README.md in place of new_repo's.
expect_readme() {
  outcome="$1" name="$2" want="$3" readme_text="$4"
  shift 4
  cases=$((cases + 1))
  new_repo
  printf '%s\n' "$readme_text" > "$repo/README.md"
  parity "$@"
  status=0
  out="$(sh "$src/scripts/parity-check.sh" "$repo" 2>&1)" || status=$?
  if [ "$outcome" = pass ] && [ "$status" -ne 0 ]; then
    echo "FAIL $name: expected success, got exit $status: $out"
    failures=$((failures + 1))
  elif [ "$outcome" = fail ] && [ "$status" -eq 0 ]; then
    echo "FAIL $name: expected failure, got success: $out"
    failures=$((failures + 1))
  elif [ -n "$want" ] && ! printf '%s\n' "$out" | grep -qF -- "$want"; then
    echo "FAIL $name: output lacks \"$want\": $out"
    failures=$((failures + 1))
  fi
}

# GitHub slugs a heading's rendered text: trailing spaces, link targets and emphasis markers
# aren't part of it. README.md's own title has links in it.
expect_readme pass "a heading with trailing spaces keeps its anchor" "" \
  "$(printf '# Title\n\n## Workflows  \n')" \
  '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows) |'
expect_readme pass "a heading with links is slugged from the link text" "" \
  '# Zig SDK [![CI](https://example.com/ci.svg)](https://example.com/ci) [docs](https://example.com/docs)' \
  '| Feature | Alpha, Beta | GA | supported | [T](README.md#zig-sdk-ci-docs) |'
expect_readme fail "a heading's link target is no anchor" "no heading for" \
  '# Zig SDK [docs](https://example.com/docs)' \
  '| Feature | Alpha, Beta | GA | supported | [T](README.md#zig-sdk-docshttpsexamplecomdocs) |'
expect_readme pass "a heading with emphasis is slugged from the rendered text" "" \
  '## _Typed_ handlers' \
  '| Feature | Alpha, Beta | GA | supported | [T](README.md#typed-handlers) |'

# The rest of GitHub's rules the check copies: a fence closes only on a fence at least as
# long, repeated slugs take the first free suffix, underscores inside words and code stay,
# `\|` is a pipe in a cell, and a sample link must name a sample that exists. The backticks
# below are Markdown, not command substitutions.
# shellcheck disable=SC2016
expect_readme fail "a heading inside a longer fence is no anchor" "no heading for #inside" \
  "$(printf '# Title\n\n````md\n```\n## Inside\n```\n````\n')" \
  '| Feature | Alpha, Beta | GA | supported | [I](README.md#inside) |'
expect_readme pass "a repeated slug takes the first free suffix" "" \
  "$(printf '## Foo\n\n## Foo\n\n## Foo 1\n')" \
  '| Feature | Alpha, Beta | GA | supported | [F](README.md#foo-1-1) |'
# shellcheck disable=SC2016
expect_readme pass "underscores inside words and code stay" "" \
  "$(printf '## snake_case and `_code`\n')" \
  '| Feature | Alpha, Beta | GA | supported | [S](README.md#snake_case-and-_code) |'
expect pass "an escaped pipe stays in its cell" "" \
  '| Pipes \| inside | Alpha, Beta | GA | supported | [S](README.md#workflows) |'
expect fail "a sample link that leaves samples/" "is not a sample" \
  '| Feature | Alpha, Beta | GA | supported | [x](samples/..) |'
expect fail "a link to samples/ itself" "is not a sample" \
  '| Feature | Alpha, Beta | GA | supported | [x](samples/) |'
expect pass "a link to a file in a sample" "" \
  '| Feature | Alpha, Beta | GA | supported | [x](samples/hello/worker.zig) |'

# What GitHub renders decides the anchors, and what the check can't be sure of fails.
# shellcheck disable=SC2016
expect_readme fail "a heading in an HTML block that runs to a blank line is no anchor" "no heading for #inner" \
  "$(printf '# Title\n\n<details>\n## Inner\n</details>\n')" \
  '| Feature | Alpha, Beta | GA | supported | [I](README.md#inner) |'
# Beyond plain text, code spans, links, and single `_` emphasis or `_` inside a word, the check
# refuses heading markup (entities, escapes, autolinks, setext headings) rather than guess.
expect_readme fail "an entity in a heading is refused" "can't be sure which headings" \
  '## Q&amp;A' \
  '| Feature | Alpha, Beta | GA | supported | [Q](README.md#qa) |'
expect_readme fail "an entity's letters are no anchor" "no heading for #qampa" \
  '## Q&amp;A' \
  '| Feature | Alpha, Beta | GA | supported | [Q](README.md#qampa) |'
# shellcheck disable=SC2016
expect_readme fail "link syntax in a code span is refused" "can't be sure which headings" \
  '## Links like `[t](u)`' \
  '| Feature | Alpha, Beta | GA | supported | [L](README.md#links-like-tu) |'
expect_readme pass "an em dash is dropped" "" \
  "$(printf '## A \342\200\224 B\n')" \
  '| Feature | Alpha, Beta | GA | supported | [A](README.md#a--b) |'
expect_readme pass "an uppercase Latin-1 letter is lowercased" "" \
  "$(printf '## \303\234ber\n')" \
  "$(printf '| Feature | Alpha, Beta | GA | supported | [U](README.md#\303\274ber) |')"
expect_readme fail "a doubled underscore is refused" "can't be sure which headings" \
  '## __Strong__ handlers' \
  '| Feature | Alpha, Beta | GA | supported | [S](README.md#strong-handlers) |'
expect_readme fail "an unpaired emphasis underscore is refused" "can't be sure which headings" \
  '## _Typed handlers' \
  '| Feature | Alpha, Beta | GA | supported | [S](README.md#_typed-handlers) |'
expect_readme fail "a backslash escape is refused" "can't be sure which headings" \
  '## foo\_bar' \
  '| Feature | Alpha, Beta | GA | supported | [F](README.md#foo_bar) |'
expect_readme fail "an autolink is refused" "can't be sure which headings" \
  '## See <https://x.com>' \
  '| Feature | Alpha, Beta | GA | supported | [S](README.md#see-httpsxcom) |'
expect_readme fail "a setext heading is refused" "can't be sure which headings" \
  "$(printf 'Setext\n======\n\nSetext\n------\n')" \
  '| Feature | Alpha, Beta | GA | supported | [S](README.md#setext-1) |'
expect_readme fail "a heading the check can't slug fails the check" "can't be sure which headings" \
  "$(printf '## Snow \342\230\203\n')" \
  '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows) |'
expect_readme fail "a supported row can't link a stub section" "is a stub" \
  "$(printf '## Later\n\n<!-- stub: temporal-sdk-zig-x -->\nNot implemented yet.\n')" \
  '| Feature | Alpha, Beta | GA | supported | [L](README.md#later) |'
expect fail "a link with a space in its destination is no link" "links a README.md section or a sample" \
  '| Feature | Alpha, Beta | GA | supported | [S](README.md#workflows extra) |'
# shellcheck disable=SC2016
expect fail "a link in a code span is no link" "links a README.md section or a sample" \
  '| Feature | Alpha, Beta | GA | supported | `[S](README.md#workflows)` |'
expect pass "a link with a title is a link" "" \
  '| Feature | Alpha, Beta | GA | supported | [S](README.md#workflows "Workflows") |'
expect fail "samples/dispatch.zig is not a sample" "no sample directory samples/dispatch.zig" \
  '| Feature | Alpha, Beta | GA | supported | [d](samples/dispatch.zig) |'
expect fail "a row names one SDK" "more than one SDK" \
  '| Feature | Alpha | GA | supported | [W](README.md#workflows) |'
expect fail "a row names an unknown SDK" "comma-separated list" \
  '| Feature | Alpha, Zeta | GA | supported | [W](README.md#workflows) |'
cases=$((cases + 1))
new_repo
parity '| Linked | all | GA | supported | [W](README.md#workflows) |'
sed 's/$/\r/' "$repo/PARITY.md" > "$repo/PARITY.crlf" && mv "$repo/PARITY.crlf" "$repo/PARITY.md"
if ! out="$(sh "$src/scripts/parity-check.sh" "$repo" 2>&1)"; then
  echo "FAIL a PARITY.md with CRLF line endings: $out"
  failures=$((failures + 1))
fi

# Every construct the anchor reader refuses makes the check fail, naming the README.md line,
# even when the link itself resolves.
# shellcheck disable=SC2016
for case in \
  "a fence not at column 0|$(printf '## Workflows\n\n  ```\n  x\n  ```\n')|README.md:3:" \
  "a comment not at column 0|$(printf '## Workflows\n\ntext <!-- hidden -->\n')|README.md:3:" \
  "two comment openers on a line|$(printf '## Workflows\n\n<!-- a --> <!--\n-->\n')|README.md:3:" \
  "an unclosed comment|$(printf '## Workflows\n\n<!--\n## Gone\n')|README.md:3:" \
  "an unclosed fence|$(printf '## Workflows\n\n```\n## Gone\n')|README.md:3:" \
  "an HTML block|$(printf '## Workflows\n\n<div>\n</div>\n')|README.md:3:" \
  "a heading after a quote marker|$(printf '## Workflows\n\n> ## Quoted\n')|README.md:3:" \
  "a heading after a list marker|$(printf '## Workflows\n\n1.     ## Coded\n')|README.md:3:" \
  "an indented heading|$(printf '## Workflows\n\n   ## Indented\n')|README.md:3:" \
  "a closing fence that is indented|$(printf '## Workflows\n\n```\n  ```\n## Maybe\n```\n')|README.md:4:" \
  "a <!--> comment|$(printf '## Workflows\n\n<!-->\n')|README.md:3:"; do
  name="${case%%|*}"; rest="${case#*|}"; text="${rest%|*}"; want="${rest##*|}"
  expect_readme fail "refused: $name" "$want" "$text" '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows) |'
done

# The row reader: GitHub keeps a pipe-less line such as `#5 retries | ...` in the table, so the
# check reads it as a row; a line that starts another block inside a table, or a row with no
# table above it, fails; a link in a comment or an image doesn't count as a row's link.
expect fail "a row starting with # is still a row" 'unknown Zig status "done"' \
  '| Linked | all | GA | supported | [W](README.md#workflows) |' \
  '#5 retries | Alpha, Beta | GA | done | [W](README.md#workflows) |'
expect fail "a block inside a table fails" "starts another block inside a table" \
  '| Linked | all | GA | supported | [W](README.md#workflows) |' \
  '> | Quoted | Alpha, Beta | GA | done | x |'
cases=$((cases + 1))
new_repo
printf '# Parity\n\n## How parity is measured\n\n%s\n\n## Feature table\n\n| Orphan | Alpha, Beta | GA | done | x |\n\n| Feature | SDKs | Stage | Zig | Where |\n|---|---|---|---|---|\n| Linked | all | GA | supported | [W](README.md#workflows) |\n' "$sdk_links" > "$repo/PARITY.md"
if out="$(sh "$src/scripts/parity-check.sh" "$repo" 2>&1)" || ! printf '%s\n' "$out" | grep -qF "no header and delimiter row"; then
  echo "FAIL a row with no table above it fails: $out"
  failures=$((failures + 1))
fi
expect fail "a link in a comment doesn't count" "links a README.md section or a sample" \
  '| Feature | Alpha, Beta | GA | supported | <!-- [W](README.md#workflows) --> |'
expect fail "an image in a row fails" "an image in a row" \
  '| Feature | Alpha, Beta | GA | supported | ![W](README.md#workflows) |'

# Round 6: every heading GitHub can render is either anchored as GitHub anchors it or refused,
# and every table row it can render in the section is either checked or refused.
expect_readme pass "a byte order mark before the first heading is dropped" "" \
  "$(printf '\357\273\277# Title\n\n## Title\n')" \
  '| Feature | Alpha, Beta | GA | supported | [T](README.md#title-1) |'
expect_readme fail "a carriage return inside a line is refused" "README.md:3:" \
  "$(printf '## Workflows\n\nIntro\r# Hidden\n')" \
  '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows) |'
expect_readme fail "an HTML heading is refused" "README.md:3:" \
  "$(printf '## Workflows\n\nSee <h2>Hidden</h2> here.\n')" \
  '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows) |'
expect_readme fail "a setext underline in a nested block quote is refused" "README.md:4:" \
  "$(printf '## Workflows\n\n> > Note\n> > ---\n')" \
  '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows) |'
expect fail "a table row inside a list item at two spaces is checked" 'unknown Zig status "done"' \
  '| Linked | all | GA | supported | [W](README.md#workflows) |' \
  '' \
  '- | Feature | SDKs | Stage | Zig | Where |' \
  '  |---|---|---|---|---|' \
  '  | Listed | Alpha, Beta | GA | done | x |'

# A supported row's links are all read: a link the check can't parse fails the row even next
# to a good one, so a broken link can't hide beside it.
expect fail "an unreadable link beside a good one fails" "a link this check can't read" \
  '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows), [b [c] d](README.md#nope) |'
expect pass "an external link beside a good one is fine" "" \
  '| Feature | Alpha, Beta | GA | supported | [W](README.md#workflows), [spec](https://example.com/spec) |'

# The real PARITY.md agrees with the code: Client.updateApiKey and Client.updateHeaders change
# the API key and headers of a connected client, so a row for that can't say it is missing.
cases=$((cases + 1))
if grep -q 'pub fn updateApiKey' "$src/src/client.zig" && grep -q 'pub fn updateHeaders' "$src/src/client.zig" &&
  grep -E '^\|.*API key or headers on a connected client' "$src/PARITY.md" | grep -qE '\|[ ]*missing[ ]*\|'; then
  echo "FAIL the real PARITY.md does not call changing the API key or headers missing: Client.updateApiKey and Client.updateHeaders exist on main"
  failures=$((failures + 1))
fi

# The real PARITY.md agrees with the code: temporal.client.uuid(arena, io) makes a version 4
# UUID from `io.random`, and a workflow's ctx.io() is seeded from the run (README.md's
# Determinism), so `client.uuid(ctx.allocator(), ctx.io())` in a workflow gives the same UUID
# on replay. The row for deterministic UUIDs can't say none of it exists. The backticks below
# are Markdown, not command substitutions.
cases=$((cases + 1))
# shellcheck disable=SC2016
if grep -q 'pub fn uuid(arena: Allocator, io: Io)' "$src/src/client.zig" &&
  grep -q 'io.random(&b);' "$src/src/client.zig" &&
  grep -q '`io.random` is seeded from the run' "$src/README.md" &&
  grep -E '^\| *Deterministic UUIDs *\|' "$src/PARITY.md" | grep -qE '\|[ ]*missing[ ]*\|'; then
  echo "FAIL the real PARITY.md does not call deterministic UUIDs missing: temporal.client.uuid with a workflow's ctx.io() is one"
  failures=$((failures + 1))
fi

# The real PARITY.md agrees with the code: Client.currentConnection is public and returns the
# connection, whose public `call` sends any RPC of the workflow, operator, test, cloud, or health
# service with a request encoded from temporal.proto. User code can make raw service calls
# today, so the row for them can't say none of it exists.
cases=$((cases + 1))
if grep -q 'pub fn currentConnection(c: \*Client) bridge.Connection' "$src/src/client.zig" &&
  grep -q '^pub fn call($' "$src/src/bridge/Connection.zig" &&
  grep -q '^    operator,$' "$src/src/bridge/Connection.zig" &&
  grep -q '^    test_service,$' "$src/src/bridge/Connection.zig" &&
  grep -q '^pub const proto = ' "$src/src/temporal.zig" &&
  grep -E '^\| *Raw gRPC service calls' "$src/PARITY.md" | grep -qE '\|[ ]*missing[ ]*\|'; then
  echo "FAIL the real PARITY.md does not call raw gRPC service calls missing: Client.currentConnection().call reaches every service"
  failures=$((failures + 1))
fi

# GitHub renders nothing inside an HTML comment, so a heading in a commented-out section of
# README.md has no anchor there, and a supported row that links it links nothing. README.md
# already uses HTML comments (`<!-- stub: ... -->`, `<!-- example: ... -->`).
expect_readme fail "a heading inside a multi-line HTML comment is no anchor" "no heading for #hidden-section" \
  "$(printf '# Title\n\n<!--\n## Hidden section\n-->\n\n## Workflows\n')" \
  '| Feature | Alpha, Beta | GA | supported | [H](README.md#hidden-section) |'

# The real PARITY.md has a row for every feature more than one official SDK offers. A custom
# failure converter (the hook that turns errors into Failure protos and back) is part of the
# data converter of six of the official SDKs PARITY.md names, GA in each, and Temporal's
# documentation has a page for it. This SDK has none (client.Options has only
# encode_failure_attributes), so without a row the GA gap list leaves it out.
cases=$((cases + 1))
if ! grep -qiE 'failure_converter|FailureConverter' "$src/src/client.zig" "$src/src/converter.zig" "$src/src/failure.zig" &&
  ! awk '/^## /{s=($0 ~ /^## Feature table/)} s' "$src/PARITY.md" | grep -qiE '^\|.*failure convert'; then
  echo "FAIL the real PARITY.md's feature table has a row for custom failure converters, a GA feature of every official SDK but PHP"
  failures=$((failures + 1))
fi

# A list marker followed by five or more spaces starts its item with an indented code block
# (CommonMark's list item rule 2, "1.     indented code"), so `-     ## Hidden` renders as
# code inside a list item, not as a heading, and GitHub gives it no anchor.
expect_readme fail "a heading-like line in a list item's indented code is no anchor" "no heading for #hidden" \
  "$(printf '# Title\n\n## Workflows\n\n-     ## Hidden\n')" \
  '| Feature | Alpha, Beta | GA | supported | [H](README.md#hidden) |'

# The real PARITY.md agrees with the code and with its own rule that a feature user code can
# reach through `Client.currentConnection().call` is partial, never missing: that call takes a
# `CallOptions` with its own `metadata`, `binary_metadata`, and `timeout_ms`, so a single call
# can carry its own gRPC metadata and RPC timeout today. The row for that can't say none of it
# exists ("no call takes its own").
cases=$((cases + 1))
if grep -q 'pub fn currentConnection(c: \*Client) bridge.Connection' "$src/src/client.zig" &&
  grep -q '^pub const CallOptions = struct {' "$src/src/bridge/Connection.zig" &&
  grep -q '^    metadata: \[\]const bytes.MetadataEntry' "$src/src/bridge/Connection.zig" &&
  grep -q '^    timeout_ms: u32 = 0,' "$src/src/bridge/Connection.zig" &&
  grep -E '^\| *RPC timeout and gRPC metadata on a single call *\|' "$src/PARITY.md" | grep -qE '\|[ ]*missing[ ]*\|'; then
  echo "FAIL the real PARITY.md does not call a single call's RPC timeout and gRPC metadata missing: Client.currentConnection().call takes CallOptions.metadata and CallOptions.timeout_ms"
  failures=$((failures + 1))
fi

# A block quote or a list item holds any blocks, setext headings among them, and GitHub
# anchors every heading on the page, so a setext heading there takes the first slug and shifts
# the suffixes of the same slug after it. Here GitHub's #note is the heading in the block quote
# (or list item) and #note-1 is the stub section, so a supported row can't link #note-1.
expect_readme fail "a setext heading in a block quote takes an anchor" "" \
  "$(printf '# Title\n\n> Note\n> ====\n\n## Note\n<!-- stub: temporal-sdk-zig-x -->\nNot implemented yet.\n\n## Note\n\nReal.\n')" \
  '| Feature | Alpha, Beta | GA | supported | [N](README.md#note-1) |'
expect_readme fail "a setext heading in a list item takes an anchor" "" \
  "$(printf '# Title\n\n1.  Note\n    ----\n\n## Note\n<!-- stub: temporal-sdk-zig-x -->\nNot implemented yet.\n\n## Note\n\nReal.\n')" \
  '| Feature | Alpha, Beta | GA | supported | [N](README.md#note-1) |'

# GitHub renders a table inside a block quote, or inside a list item whose content is indented
# four spaces, as a table, so a feature row there is in the feature table and must be checked
# (or refused): an unknown status in it can't pass.
expect fail "a feature table inside a block quote is checked" "" \
  '| Linked | all | GA | supported | [W](README.md#workflows) |' \
  '' \
  '> | Feature | SDKs | Stage | Zig | Where |' \
  '> |---|---|---|---|---|' \
  '> | Quoted | Alpha, Beta | GA | done | x |'
expect fail "a feature table inside a list item is checked" "" \
  '| Linked | all | GA | supported | [W](README.md#workflows) |' \
  '' \
  '1.  Note' \
  '' \
  '    | Feature | SDKs | Stage | Zig | Where |' \
  '    |---|---|---|---|---|' \
  '    | Listed | Alpha, Beta | GA | done | x |'

# A link destination in <...> may hold spaces, and is one destination: GitHub links
# README.md#workflows%20foo, which no heading has, not README.md#workflows. The same goes for a
# sample path with a space in it.
expect fail "a link destination with a space in <...> is one target" "" \
  '| Feature | Alpha, Beta | GA | supported | [W](<README.md#workflows foo>) |'
expect fail "a sample path with a space in <...> is one target" "" \
  '| Feature | Alpha, Beta | GA | supported | [W](<samples/hello nope>) |'
# A link destination is a path, not a shell pattern: samples/hel* names no sample, even when
# the check runs from the repository root, as zig build readme-check runs it.
cases=$((cases + 1))
new_repo
parity '| Feature | Alpha, Beta | GA | supported | [W](samples/hel*) |'
if out="$(cd "$repo" && sh "$src/scripts/parity-check.sh" "$repo" 2>&1)"; then
  echo "FAIL a sample link with a glob character names no sample: expected failure, got success: $out"
  failures=$((failures + 1))
fi

# The real PARITY.md lists a feature as offered by one SDK only when no other official SDK
# offers it. The enhanced stack trace query (`__enhanced_stack_trace`) is answered by the
# workers of two official SDKs (PARITY.md's row names them), and the Zig SDK
# answers neither it nor `__stack_trace` (README.md's stub, temporal-sdk-zig-d51.6.9). So it is
# a parity requirement: it needs a row in the feature table, and the GA gap list must show it.
cases=$((cases + 1))
if ! grep -q '__enhanced_stack_trace' "$src/src/workflow.zig" "$src/src/workflow/Instance.zig" "$src/src/worker.zig" &&
  ! awk '/^## /{s=($0 ~ /^## Feature table/)} s' "$src/PARITY.md" | grep -qiE '^\|.*enhanced stack trace'; then
  echo "FAIL the real PARITY.md's feature table has a row for the enhanced stack trace query, which TypeScript and Python both offer"
  failures=$((failures + 1))
fi

# Likewise a remote codec client, a payload codec that sends payloads to a codec server to
# encode and decode them: two official SDKs ship one (PARITY.md's row names them). The Zig
# SDK has none (README.md's stub,
# temporal-sdk-zig-d51.4.4), so it is a parity requirement with a row in the feature table.
cases=$((cases + 1))
if grep -q 'stub: temporal-sdk-zig-d51.4.4' "$src/README.md" &&
  ! awk '/^## /{s=($0 ~ /^## Feature table/)} s' "$src/PARITY.md" | grep -qiE '^\|.*remote codec'; then
  echo "FAIL the real PARITY.md's feature table has a row for a remote codec client, which Go and Java both offer"
  failures=$((failures + 1))
fi

if [ "$failures" -gt 0 ]; then
  echo "parity-check-test: $failures of $cases cases failed"
  exit 1
fi
echo "parity-check-test: $cases cases passed"
