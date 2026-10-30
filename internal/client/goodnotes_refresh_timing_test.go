package client

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/config"
	"github.com/centrifugal/centrifugo/v6/internal/metrics"
	"github.com/centrifugal/centrifugo/v6/internal/proxyproto"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var timingKeys = []string{"after_known", "after_ms", "armed_state_unchanged", "branch", "build", "cause_seq",
	"copied_state_still_current", "current_arm_seq", "delay_known", "delay_s", "dropped_before", "event",
	"expiry_reached_observer_clock", "handler_present", "has_arm", "k", "level", "message",
	"monotonic_due", "new_expiry_sha256", "origin", "predicate_strict_gt", "prior_expiry_sha256",
	"scope", "server_side", "since_arm_known", "since_arm_ms", "state_seq", "subscription_present",
	"timer_dispatch", "witness"}

func captureTiming(t *testing.T) *lockedBuffer {
	t.Helper()
	out := &lockedBuffer{}
	logger := zerolog.New(out)
	prevEnabled, prevLogger := refreshWitnessEnabled, refreshWitnessLogger
	refreshWitnessEnabled = true
	refreshWitnessLogger = func() *zerolog.Logger { return &logger }
	t.Cleanup(func() { refreshWitnessEnabled, refreshWitnessLogger = prevEnabled, prevLogger })
	return out
}

func witnessLines(b *lockedBuffer, witness string) []map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["witness"] == witness {
			out = append(out, m)
		}
	}
	return out
}

func keysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestGoodnotesRefreshTimingLineIsClosedAndContentFree(t *testing.T) {
	out := captureTiming(t)
	emitRefreshTiming(centrifuge.GoodnotesTimingEvent{Kind: centrifuge.GoodnotesNaturalTrigger, ClientID: "private-client",
		StateSeq: 3, CauseSeq: 7, CurrentArmSeq: 2, Origin: centrifuge.GoodnotesOriginConnect, PriorExpire: 1793000000,
		TimerDispatch: true, ServerSide: true, HandlerPresent: true, HasArm: true, ArmAfter: 1500 * time.Millisecond,
		SinceArm: 1600 * time.Millisecond, ArmedStateUnchanged: true, MonotonicDue: true, ExpiryReachedObs: true})
	emitRefreshTiming(centrifuge.GoodnotesTimingEvent{Kind: centrifuge.GoodnotesSubNaturalTrigger, ClientID: "private-client",
		Channel: "textdoc-presence:private-doc", CauseSeq: 8, PriorExpire: 1793000020, PredicateStrictGT: true, DelaySeconds: 25,
		CopiedStateStillCurrent: true, SubscriptionPresent: true})
	emitRefreshTiming(centrifuge.GoodnotesTimingEvent{Kind: centrifuge.GoodnotesStateRecorded, ClientID: "private-client",
		Origin: centrifuge.GoodnotesStateOrigin(99), HasArm: true, ArmAfter: -time.Second, SinceArm: 400 * 24 * time.Hour})
	emitRefreshTiming(centrifuge.GoodnotesTimingEvent{Kind: centrifuge.GoodnotesTimingKind(99), ClientID: "private-client"})
	emitRefreshTiming(centrifuge.GoodnotesTimingEvent{Kind: centrifuge.GoodnotesSubStateApplied, ClientID: "private-client"})
	raw := out.raw()
	for _, secret := range []string{"private-client", "private-doc", "textdoc-presence", "1793000000", "1793000020"} {
		require.NotContains(t, raw, secret)
	}
	lines := witnessLines(out, "goodnotes.refresh-timing.v1")
	require.Len(t, lines, 3, "unknown kind and channel-less subscription events are skipped")
	for _, l := range lines {
		require.Equal(t, timingKeys, keysOf(l))
		require.Equal(t, "goodnotes refresh timing", l["message"])
	}
	conn, sub, odd := lines[0], lines[1], lines[2]
	require.Equal(t, "connection", conn["scope"])
	require.Equal(t, refreshCorrelationSHA256("private-client"), conn["k"])
	require.Equal(t, refreshExpirySHA256("private-client", 1793000000), conn["prior_expiry_sha256"])
	require.EqualValues(t, 1500, conn["after_ms"])
	require.Equal(t, true, conn["after_known"])
	require.EqualValues(t, 7, conn["cause_seq"])
	require.Equal(t, "connect", conn["origin"])
	require.Equal(t, "subscription", sub["scope"])
	require.Equal(t, subRefreshCorrelationSHA256("private-client", "textdoc-presence:private-doc"), sub["k"])
	require.EqualValues(t, 25, sub["delay_s"])
	require.Equal(t, true, sub["delay_known"])
	require.Equal(t, "unknown", odd["origin"])
	require.Equal(t, false, odd["after_known"])
	require.Equal(t, false, odd["since_arm_known"])
	require.EqualValues(t, 0, odd["after_ms"])
}

func TestGoodnotesRefreshTimingInstallIsOffByDefaultAndPanicSafe(t *testing.T) {
	prev := refreshWitnessEnabled
	t.Cleanup(func() { refreshWitnessEnabled = prev })
	refreshWitnessEnabled = false
	installRefreshTimingObserver(panickyTimingTarget{}) // not even called
	refreshWitnessEnabled = true
	require.NotPanics(t, func() { installRefreshTimingObserver(panickyTimingTarget{}) })
}

type panickyTimingTarget struct{}

func (panickyTimingTarget) SetGoodnotesTimingObserver(chan<- centrifuge.GoodnotesTimingEvent) {
	panic("broken")
}

// End-to-end: real Handler.Setup installs the typed sink in OnConnect, the node's own expire
// timer dispatches, and the timing chain plus the v1 applied line come out of the real drain.
func runTimingE2E(t *testing.T, reply func() (*proxyproto.RefreshResponse, error), want func([]map[string]any) bool) (string, []map[string]any, []map[string]any) {
	t.Helper()
	e2eMetricsOnce.Do(func() { _ = metrics.Init(metrics.Config{Registerer: prometheus.NewRegistry()}) })
	out := captureTiming(t)
	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	h := NewHandler(node, cfgContainer, nil, nil, &ProxyMap{
		ConnectProxy: e2eConnectProxy{expireAt: time.Now().Unix() + 1},
		RefreshProxy: &e2eRefreshProxy{reply: reply},
	})
	require.NoError(t, h.Setup())
	client, closeFn, err := centrifuge.NewClient(context.Background(), node, tools.NewTestTransport())
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	cmd := &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}
	require.True(t, client.HandleCommand(cmd, cmd.SizeVT()))
	id := client.ID()
	k := refreshCorrelationSHA256(id)
	var mine []map[string]any
	require.Eventually(t, func() bool {
		mine = mine[:0]
		for _, l := range witnessLines(out, "goodnotes.refresh-timing.v1") {
			if l["k"] == k {
				mine = append(mine, l)
			}
		}
		return want(mine)
	}, 6*time.Second, 20*time.Millisecond)
	require.NotContains(t, out.raw(), id)
	return id, mine, witnessLines(out, "goodnotes.refresh-applied.v1")
}

func timingEvent(lines []map[string]any, event string) map[string]any {
	for _, l := range lines {
		if l["event"] == event {
			return l
		}
	}
	return nil
}

func hasEvents(events ...string) func([]map[string]any) bool {
	return func(lines []map[string]any) bool {
		for _, e := range events {
			if timingEvent(lines, e) == nil {
				return false
			}
		}
		return true
	}
}

func TestGoodnotesRefreshTimingE2ENaturalChainJoinsAppliedWitness(t *testing.T) {
	backend := time.Now().Unix() + 600
	id, lines, applied := runTimingE2E(t, func() (*proxyproto.RefreshResponse, error) {
		return &proxyproto.RefreshResponse{Result: &proxyproto.RefreshResult{ExpireAt: backend}}, nil
	}, hasEvents("state-observed", "natural-trigger", "state-recorded"))
	observed, trig, rec := timingEvent(lines, "state-observed"), timingEvent(lines, "natural-trigger"), timingEvent(lines, "state-recorded")
	require.Equal(t, "connect", observed["origin"])
	require.Equal(t, observed["prior_expiry_sha256"], trig["prior_expiry_sha256"], "trigger used the observed applied deadline")
	require.Equal(t, observed["state_seq"], trig["state_seq"])
	for _, f := range []string{"timer_dispatch", "server_side", "handler_present", "has_arm", "armed_state_unchanged",
		"monotonic_due", "expiry_reached_observer_clock", "after_known", "since_arm_known"} {
		require.Equal(t, true, trig[f], f)
	}
	require.GreaterOrEqual(t, trig["since_arm_ms"].(float64), trig["after_ms"].(float64))
	require.Equal(t, "natural-server-refresh", rec["origin"])
	require.Equal(t, trig["cause_seq"], rec["cause_seq"])
	require.Equal(t, trig["prior_expiry_sha256"], rec["prior_expiry_sha256"])
	require.Equal(t, refreshExpirySHA256(id, backend), rec["new_expiry_sha256"])
	require.EqualValues(t, trig["state_seq"].(float64)+1, rec["state_seq"])
	// Joins the v1 receiver line: same K, decoded backend outcome, same applied commitment.
	require.Len(t, applied, 1)
	require.Equal(t, rec["k"], applied[0]["k"])
	require.Equal(t, "backendExpireAt", applied[0]["outcome"])
	require.Equal(t, rec["new_expiry_sha256"], applied[0]["applied_expiry_sha256"])
}

func TestGoodnotesRefreshTimingE2EGraceAndExpiredStayTyped(t *testing.T) {
	_, grace, graceApplied := runTimingE2E(t, func() (*proxyproto.RefreshResponse, error) {
		return nil, errors.New("backend unreachable")
	}, hasEvents("natural-trigger", "state-recorded"))
	require.Equal(t, timingEvent(grace, "natural-trigger")["cause_seq"], timingEvent(grace, "state-recorded")["cause_seq"])
	require.Len(t, graceApplied, 1)
	require.Equal(t, "proxyErrorGrace60", graceApplied[0]["outcome"], "the assignment is Centrifugo's grace, not a Backend decision")

	_, expired, _ := runTimingE2E(t, func() (*proxyproto.RefreshResponse, error) {
		return &proxyproto.RefreshResponse{Result: &proxyproto.RefreshResult{Expired: true}}, nil
	}, hasEvents("natural-trigger", "not-applied"))
	na := timingEvent(expired, "not-applied")
	require.Equal(t, "expired", na["branch"])
	require.Equal(t, timingEvent(expired, "natural-trigger")["cause_seq"], na["cause_seq"])
	require.Nil(t, timingEvent(expired, "state-recorded"))
}

func TestGoodnotesRefreshTimingDelayBoundsNeverOverflow(t *testing.T) {
	maxSeconds := int64(maxWitnessDuration / time.Second)
	for _, tc := range []struct {
		name  string
		delay int64
		known bool
	}{
		{"zero", 0, true},
		{"exactly max", maxSeconds, true},
		{"just over max", maxSeconds + 1, false},
		{"huge positive that overflows a Duration", math.MaxInt64 / 1000, false},
		{"max int64", math.MaxInt64, false},
		{"negative", -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := captureTiming(t)
			emitRefreshTiming(centrifuge.GoodnotesTimingEvent{Kind: centrifuge.GoodnotesSubNaturalTrigger,
				ClientID: "c", Channel: "ch", CauseSeq: 1, PredicateStrictGT: true, DelaySeconds: tc.delay})
			lines := witnessLines(out, "goodnotes.refresh-timing.v1")
			require.Len(t, lines, 1)
			require.Equal(t, tc.known, lines[0]["delay_known"])
			if tc.known {
				require.EqualValues(t, tc.delay, lines[0]["delay_s"], "known values are emitted as-is")
			} else {
				require.EqualValues(t, 0, lines[0]["delay_s"], "unknown is reported, not clipped to the bound")
			}
		})
	}
}
