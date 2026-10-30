#!/usr/bin/env bash
# Reviewer check for the vendored centrifuge source closure (Goodnotes refresh witnesses v1).
#
# third_party/centrifuge must equal the official github.com/centrifugal/centrifuge commit
# 84d40cc4dbf6d342fe907d69fd07ca677039bfe3 (pinned by upstream centrifugo v6.9.1 go.mod), minus
# the excluded upstream-tracked _examples/ subtree (example programs plus committed Mach-O builds,
# never part of the Go build: underscore directories are ignored), plus exactly the ADDED files,
# with client.go replaced by the official client.go plus the pinned reviewable patch
# third_party/goodnotes-centrifuge-client.go.patch (refresh-timing witness hooks).
# Needs a local clone of the official centrifuge repository; performs no network access itself
# (GIT_NO_LAZY_FETCH=1) and writes no objects or refs into either repository: every object a check
# creates (temporary index blobs/trees) goes to a temporary object directory, with the original
# object stores attached read-only as alternates.
set -euo pipefail
OFFICIAL_COMMIT=84d40cc4dbf6d342fe907d69fd07ca677039bfe3
OFFICIAL_TREE=678c1f96a98a96c4078d099de6a8060345b8aff0
EXCLUDED=_examples
EXCLUDED_TREE=5ec09f18308526baa26fac2bc9ff7f74d0b42a7e
EXPECTED_TREE=4f95b599b8155a3eaa099934add48164f62dace5
ADDED=(goodnotes_applied_refresh_state.go goodnotes_applied_refresh_state_test.go
       goodnotes_timing_observer.go goodnotes_timing_observer_test.go)
OFFICIAL_CLIENT_BLOB=c6a42b294d7b9f09a9f72e421ffe62f8bb2fa09c
PATCHED_CLIENT_BLOB=1d4b2cbade2947089cdc9ff73baeb410061f6b04
PATCH_SHA256=648cb97eb3037afca18c82222a1ab35a2ff1cf621e67f4bdebab20df81d835ba
clone=${1:?usage: verify-centrifuge-closure.sh <local official centrifuge clone>}
root=$(git rev-parse --show-toplevel)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
export GIT_NO_LAZY_FETCH=1
clone_objects=$(git -C "$clone" rev-parse --path-format=absolute --git-path objects)
root_objects=$(git -C "$root" rev-parse --path-format=absolute --git-path objects)
mkdir -p "$tmp/clone-odb" "$tmp/root-odb"
# Isolated writers: new objects land in $tmp only; existing ones are read through alternates.
cgit() { GIT_OBJECT_DIRECTORY="$tmp/clone-odb" GIT_ALTERNATE_OBJECT_DIRECTORIES="$clone_objects" git -C "$clone" "$@"; }
rgit() { GIT_OBJECT_DIRECTORY="$tmp/root-odb" GIT_ALTERNATE_OBJECT_DIRECTORIES="$root_objects" git -C "$root" "$@"; }

test "$(git -C "$clone" rev-parse "$OFFICIAL_COMMIT^{tree}")" = "$OFFICIAL_TREE"
test "$(git -C "$clone" rev-parse "$OFFICIAL_COMMIT:$EXCLUDED")" = "$EXCLUDED_TREE"
GIT_INDEX_FILE="$tmp/official" cgit read-tree "$OFFICIAL_COMMIT"
GIT_INDEX_FILE="$tmp/official" cgit rm -r -q --cached -- "$EXCLUDED"
test "$(GIT_INDEX_FILE="$tmp/official" cgit write-tree)" = "$EXPECTED_TREE"

# The only modified upstream file: official client.go + pinned patch must reproduce the vendored bytes.
patch_file="$root/third_party/goodnotes-centrifuge-client.go.patch"
test "$(shasum -a 256 "$patch_file" | cut -c1-64)" = "$PATCH_SHA256"
test "$(git -C "$clone" rev-parse "$OFFICIAL_COMMIT:client.go")" = "$OFFICIAL_CLIENT_BLOB"
git -C "$clone" cat-file blob "$OFFICIAL_CLIENT_BLOB" > "$tmp/client.go"
patch -s "$tmp/client.go" < "$patch_file"
test "$(git hash-object "$tmp/client.go")" = "$PATCHED_CLIENT_BLOB"
test "$(git -C "$root" hash-object third_party/centrifuge/client.go)" = "$PATCHED_CLIENT_BLOB"
GIT_INDEX_FILE="$tmp/official" cgit update-index --cacheinfo "100644,$PATCHED_CLIENT_BLOB,client.go"
EXPECTED_PATCHED_TREE=$(GIT_INDEX_FILE="$tmp/official" cgit write-tree --missing-ok)

test ! -e "$root/third_party/centrifuge/$EXCLUDED"
GIT_INDEX_FILE="$tmp/vendored" rgit read-tree --empty
GIT_INDEX_FILE="$tmp/vendored" rgit add -f -- third_party/centrifuge
for f in "${ADDED[@]}"; do GIT_INDEX_FILE="$tmp/vendored" rgit rm -q --cached -- "third_party/centrifuge/$f"; done
actual=$(GIT_INDEX_FILE="$tmp/vendored" rgit write-tree --prefix=third_party/centrifuge/)
test "$actual" = "$EXPECTED_PATCHED_TREE" || { echo "vendored tree $actual != expected $EXPECTED_PATCHED_TREE" >&2; exit 1; }
echo "ok: third_party/centrifuge = official $OFFICIAL_COMMIT minus $EXCLUDED/ ($EXPECTED_TREE) + pinned client.go patch ($PATCHED_CLIENT_BLOB) = $EXPECTED_PATCHED_TREE + ${#ADDED[@]} Goodnotes files"
