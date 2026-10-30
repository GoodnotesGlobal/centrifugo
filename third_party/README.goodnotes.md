# Vendored centrifuge source closure (Goodnotes refresh-applied witness v1)

`third_party/centrifuge/` is the official source of `github.com/centrifugal/centrifuge`, as a strict manifest:

- commit `84d40cc4dbf6d342fe907d69fd07ca677039bfe3`, the pseudo-version
  `v0.38.1-0.20260711082514-84d40cc4dbf6` that official centrifugo `v6.9.1`
  (`4603be29243501f4ac2787de17c4f0428b27864e`) requires in `go.mod`;
- official tree `678c1f96a98a96c4078d099de6a8060345b8aff0`, **minus** the upstream-tracked
  `_examples/` subtree (`5ec09f18308526baa26fac2bc9ff7f74d0b42a7e`). That subtree holds example programs plus
  upstream-committed Mach-O builds, `.DS_Store` and GIFs; Go ignores `_`-prefixed directories, so it is
  not part of the build. Expected vendored tree before additions: `4f95b599b8155a3eaa099934add48164f62dace5`;
- MIT License (`third_party/centrifuge/LICENSE`, unchanged).

It adds exactly four files:

- `goodnotes_applied_refresh_state.go`: read-only accessors `AppliedRefreshState()` and
  `AppliedSubscriptionState(channel)` (applied witness v1);
- `goodnotes_timing_observer.go`: the refresh-timing witness types, typed send-only sink and helpers;
- their two `_test.go` files.

It modifies exactly one upstream file, `client.go`: official blob `c6a42b294d7b9f09a9f72e421ffe62f8bb2fa09c`
plus the reviewable patch `third_party/goodnotes-centrifuge-client.go.patch` (sha256 pinned in the
verify script) gives blob `1d4b2cbade2947089cdc9ff73baeb410061f6b04`. The patch only adds observation hook
calls: a sidecar field, expire-dispatch source, expire-arm snapshot, and records at the existing expiration
writers and sub-refresh branches. No timer value, assignment, branch, error or close code changes.

The root `go.mod` uses `replace github.com/centrifugal/centrifuge => ./third_party/centrifuge`.
No other module is replaced and nothing is downloaded or patched at build time. Verify with
`third_party/verify-centrifuge-closure.sh <local official centrifuge clone>`.
