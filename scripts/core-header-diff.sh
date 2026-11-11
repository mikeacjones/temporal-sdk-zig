#!/usr/bin/env bash
# Shows how the sdk-core C bridge header changes between the pinned tag and another tag.
# Usage: scripts/core-header-diff.sh <new-tag>
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
new="${1:?usage: scripts/core-header-diff.sh <new-tag>}"
old="$(tr -d '[:space:]' < "$root/CORE_VERSION")"
header="crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h"

git -C "$root/deps/sdk-core" fetch --quiet --depth 1 origin tag "$old" tag "$new"
git -C "$root/deps/sdk-core" diff --stat "$old" "$new" -- "$header" crates/protos
git -C "$root/deps/sdk-core" diff "$old" "$new" -- "$header"
