#!/usr/bin/env bash
# Reviewer check for the vendored centrifuge source closure (Goodnotes refresh-applied witness v1).
#
# third_party/centrifuge must equal the official github.com/centrifugal/centrifuge commit
# 84d40cc4dbf6d342fe907d69fd07ca677039bfe3 (pinned by upstream centrifugo v6.9.1 go.mod), minus
# the excluded upstream-tracked _examples/ subtree (example programs plus committed Mach-O builds,
# never part of the Go build: underscore directories are ignored), plus exactly the ADDED files.
# Needs a local clone of the official centrifuge repository; performs no network access itself.
set -euo pipefail
OFFICIAL_COMMIT=84d40cc4dbf6d342fe907d69fd07ca677039bfe3
OFFICIAL_TREE=678c1f96a98a96c4078d099de6a8060345b8aff0
EXCLUDED=_examples
EXCLUDED_TREE=5ec09f18308526baa26fac2bc9ff7f74d0b42a7e
EXPECTED_TREE=4f95b599b8155a3eaa099934add48164f62dace5
ADDED=(goodnotes_applied_refresh_state.go goodnotes_applied_refresh_state_test.go)
clone=${1:?usage: verify-centrifuge-closure.sh <local official centrifuge clone>}
root=$(git rev-parse --show-toplevel)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

test "$(git -C "$clone" rev-parse "$OFFICIAL_COMMIT^{tree}")" = "$OFFICIAL_TREE"
test "$(git -C "$clone" rev-parse "$OFFICIAL_COMMIT:$EXCLUDED")" = "$EXCLUDED_TREE"
GIT_INDEX_FILE="$tmp/official" git -C "$clone" read-tree "$OFFICIAL_COMMIT"
GIT_INDEX_FILE="$tmp/official" git -C "$clone" rm -r -q --cached -- "$EXCLUDED"
test "$(GIT_INDEX_FILE="$tmp/official" git -C "$clone" write-tree)" = "$EXPECTED_TREE"

test ! -e "$root/third_party/centrifuge/$EXCLUDED"
GIT_INDEX_FILE="$tmp/vendored" git -C "$root" read-tree --empty
GIT_INDEX_FILE="$tmp/vendored" git -C "$root" add -f -- third_party/centrifuge
for f in "${ADDED[@]}"; do GIT_INDEX_FILE="$tmp/vendored" git -C "$root" rm -q --cached -- "third_party/centrifuge/$f"; done
actual=$(GIT_INDEX_FILE="$tmp/vendored" git -C "$root" write-tree --prefix=third_party/centrifuge/)
test "$actual" = "$EXPECTED_TREE" || { echo "vendored tree $actual != expected $EXPECTED_TREE" >&2; exit 1; }
echo "ok: third_party/centrifuge = official $OFFICIAL_COMMIT minus $EXCLUDED/ ($EXPECTED_TREE) + ${#ADDED[@]} Goodnotes files"
