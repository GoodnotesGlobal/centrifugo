package client

import (
	"sync"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/build"

	"github.com/centrifugal/centrifuge"
)

// Goodnotes refresh-timing witness v1.
//
// The SDK hands raw timing/state observations to one process-wide bounded channel with a
// non-blocking send (full = dropped, counted per client). A single drain goroutine hashes and
// logs each event inside its own panic guard. Off unless GOODNOTES_SPARSE_REFRESH_WITNESS_ENABLED
// is "true". Only pseudonymous digests, closed enums, booleans, sequences and bounded durations
// are logged: never a raw client ID, channel, expiration, user, token, info or payload.
// The chain is local to one client: state -> natural trigger -> state assignment linked by
// cause_seq. current_arm_seq is the current schedule, not the arm that fired. Missing, dropped,
// early, overwritten or stale observations leave the cause unknown.

const (
	refreshTimingBuffer = 4096
	maxWitnessDuration  = 366 * 24 * time.Hour // outside [0, 366d] a duration is reported unknown
)

var (
	refreshTimingOnce sync.Once
	refreshTimingSink chan centrifuge.GoodnotesTimingEvent
)

func refreshTimingChannel() chan<- centrifuge.GoodnotesTimingEvent {
	refreshTimingOnce.Do(func() {
		refreshTimingSink = make(chan centrifuge.GoodnotesTimingEvent, refreshTimingBuffer)
		go func() {
			for ev := range refreshTimingSink {
				emitRefreshTiming(ev)
			}
		}()
	})
	return refreshTimingSink
}

type refreshTimingTarget interface {
	SetGoodnotesTimingObserver(chan<- centrifuge.GoodnotesTimingEvent)
}

func installRefreshTimingObserver(c refreshTimingTarget) {
	if !refreshWitnessEnabled {
		return
	}
	defer func() { _ = recover() }()
	c.SetGoodnotesTimingObserver(refreshTimingChannel())
}

var timingKindNames = map[centrifuge.GoodnotesTimingKind]string{
	centrifuge.GoodnotesStateObserved:     "state-observed",
	centrifuge.GoodnotesStateRecorded:     "state-recorded",
	centrifuge.GoodnotesNaturalTrigger:    "natural-trigger",
	centrifuge.GoodnotesNotApplied:        "not-applied",
	centrifuge.GoodnotesSubNaturalTrigger: "sub-natural-trigger",
	centrifuge.GoodnotesSubStateApplied:   "sub-state-applied",
	centrifuge.GoodnotesSubNotApplied:     "sub-not-applied",
}

var timingOriginNames = map[centrifuge.GoodnotesStateOrigin]string{
	centrifuge.GoodnotesOriginUnknown:              "unknown",
	centrifuge.GoodnotesOriginConnect:              "connect",
	centrifuge.GoodnotesOriginNaturalServerRefresh: "natural-server-refresh",
	centrifuge.GoodnotesOriginClientCommand:        "client-command",
	centrifuge.GoodnotesOriginServerAPI:            "server-api",
}

var timingBranchNames = map[centrifuge.GoodnotesNotAppliedBranch]string{
	centrifuge.GoodnotesBranchNone:                  "none",
	centrifuge.GoodnotesBranchHandlerError:          "handler-error",
	centrifuge.GoodnotesBranchDisconnect:            "disconnect",
	centrifuge.GoodnotesBranchExpired:               "expired",
	centrifuge.GoodnotesBranchZeroExpiration:        "zero-expiration",
	centrifuge.GoodnotesBranchPastExpiration:        "past-expiration",
	centrifuge.GoodnotesBranchChannelMissing:        "channel-missing",
	centrifuge.GoodnotesBranchClientSideOrNoHandler: "client-side-or-no-handler",
}

func enumName[T comparable](names map[T]string, v T) string {
	if name, ok := names[v]; ok {
		return name
	}
	return "unknown"
}

func boundedMillis(d time.Duration) (int64, bool) {
	if d < 0 || d > maxWitnessDuration {
		return 0, false
	}
	return d.Milliseconds(), true
}

func emitRefreshTiming(ev centrifuge.GoodnotesTimingEvent) {
	defer func() { _ = recover() }()
	kind, ok := timingKindNames[ev.Kind]
	if !ok || ev.ClientID == "" {
		return
	}
	subscription := ev.Kind == centrifuge.GoodnotesSubNaturalTrigger ||
		ev.Kind == centrifuge.GoodnotesSubStateApplied || ev.Kind == centrifuge.GoodnotesSubNotApplied
	scope, k := "connection", refreshCorrelationSHA256(ev.ClientID)
	prior, assigned := refreshExpirySHA256(ev.ClientID, ev.PriorExpire), refreshExpirySHA256(ev.ClientID, ev.NewExpire)
	if subscription {
		if ev.Channel == "" {
			return
		}
		scope, k = "subscription", subRefreshCorrelationSHA256(ev.ClientID, ev.Channel)
		prior = subRefreshExpirySHA256(ev.ClientID, ev.Channel, ev.PriorExpire)
		assigned = subRefreshExpirySHA256(ev.ClientID, ev.Channel, ev.NewExpire)
	}
	afterMs, afterKnown := boundedMillis(ev.ArmAfter)
	sinceMs, sinceKnown := boundedMillis(ev.SinceArm)
	// Compare whole seconds directly: converting a large int64 to a Duration would overflow.
	delayKnown := ev.DelaySeconds >= 0 && ev.DelaySeconds <= int64(maxWitnessDuration/time.Second)
	delayS := ev.DelaySeconds
	if !delayKnown {
		delayS = 0
	}
	refreshWitnessLogger().Info().
		Str("witness", "goodnotes.refresh-timing.v1").
		Str("scope", scope).
		Str("event", kind).
		Str("k", k).
		Uint64("state_seq", ev.StateSeq).
		Uint64("cause_seq", ev.CauseSeq).
		Uint64("current_arm_seq", ev.CurrentArmSeq).
		Str("origin", enumName(timingOriginNames, ev.Origin)).
		Str("branch", enumName(timingBranchNames, ev.Branch)).
		Str("prior_expiry_sha256", prior).
		Str("new_expiry_sha256", assigned).
		Bool("timer_dispatch", ev.TimerDispatch).
		Bool("server_side", ev.ServerSide).
		Bool("handler_present", ev.HandlerPresent).
		Bool("has_arm", ev.HasArm).
		Bool("armed_state_unchanged", ev.ArmedStateUnchanged).
		Bool("monotonic_due", ev.MonotonicDue).
		Bool("expiry_reached_observer_clock", ev.ExpiryReachedObs).
		Int64("after_ms", afterMs).
		Bool("after_known", ev.HasArm && afterKnown).
		Int64("since_arm_ms", sinceMs).
		Bool("since_arm_known", ev.HasArm && sinceKnown).
		Bool("predicate_strict_gt", ev.PredicateStrictGT).
		Int64("delay_s", delayS).
		Bool("delay_known", subscription && ev.Kind == centrifuge.GoodnotesSubNaturalTrigger && delayKnown).
		Bool("copied_state_still_current", ev.CopiedStateStillCurrent).
		Bool("subscription_present", ev.SubscriptionPresent).
		Uint64("dropped_before", ev.DroppedBefore).
		Str("build", build.Version).
		Msg("goodnotes refresh timing")
}
