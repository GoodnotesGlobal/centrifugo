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

It adds exactly two files and modifies none:

- `goodnotes_applied_refresh_state.go`: read-only accessors `AppliedRefreshState()` and
  `AppliedSubscriptionState(channel)`;
- `goodnotes_applied_refresh_state_test.go`.

The root `go.mod` uses `replace github.com/centrifugal/centrifuge => ./third_party/centrifuge`.
No other module is replaced and nothing is downloaded or patched at build time. Verify with
`third_party/verify-centrifuge-closure.sh <local official centrifuge clone>`.
