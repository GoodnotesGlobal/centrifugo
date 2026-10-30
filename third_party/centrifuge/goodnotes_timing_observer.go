package centrifuge

import (
	"sync/atomic"
	"time"
)

// Goodnotes refresh-timing witness v1 (vendored addition to official 84d40cc4dbf6).
//
// Observability only. With no sink installed every hook returns immediately and no extra clock is
// read. With a sink, hooks copy plain values and hand them to a typed send-only channel with a
// non-blocking select: a full channel drops the event (counted), a closed channel's panic is
// recovered here. No callback ever runs, so hooks are safe while Client.mu or presenceMu is held.
// Timer values, state assignments, branches, errors and close codes are never changed.
//
// Sequences are local to one Client. CurrentArmSeq is the schedule selected when the expire timer
// was last armed; it is NOT proof of which historical arm delivered a callback. Any dropped,
// overwritten, early, stale or missing observation leaves the cause unknown.

// GoodnotesTimingKind names an observation.
type GoodnotesTimingKind uint8

const (
	GoodnotesStateObserved     GoodnotesTimingKind = iota + 1 // stored state when the sink was installed
	GoodnotesStateRecorded                                    // a writer assigned connection expiration
	GoodnotesNaturalTrigger                                   // expire() reached for this connection
	GoodnotesNotApplied                                       // a natural refresh assigned nothing
	GoodnotesSubNaturalTrigger                                // presence expiration check took the expired branch
	GoodnotesSubStateApplied                                  // sub refresh callback assigned channel expiration
	GoodnotesSubNotApplied                                    // sub refresh callback assigned nothing
)

// GoodnotesStateOrigin names the writer of a connection expiration.
type GoodnotesStateOrigin uint8

const (
	GoodnotesOriginUnknown GoodnotesStateOrigin = iota
	GoodnotesOriginConnect
	GoodnotesOriginNaturalServerRefresh
	GoodnotesOriginClientCommand
	GoodnotesOriginServerAPI
)

// GoodnotesNotAppliedBranch names why a refresh callback assigned nothing.
type GoodnotesNotAppliedBranch uint8

const (
	GoodnotesBranchNone GoodnotesNotAppliedBranch = iota
	GoodnotesBranchHandlerError
	GoodnotesBranchDisconnect
	GoodnotesBranchExpired
	GoodnotesBranchZeroExpiration
	GoodnotesBranchPastExpiration
	GoodnotesBranchChannelMissing
	GoodnotesBranchClientSideOrNoHandler
)

// GoodnotesTimingEvent carries raw in-process values; the receiver must hash before persisting.
type GoodnotesTimingEvent struct {
	Kind     GoodnotesTimingKind
	ClientID string
	Channel  string // subscription events only

	StateSeq    uint64
	Origin      GoodnotesStateOrigin
	CauseSeq    uint64
	PriorExpire int64 // value the decision used (connection: stored c.exp; subscription: copied context)
	NewExpire   int64 // value assigned (StateRecorded / SubStateApplied)
	Branch      GoodnotesNotAppliedBranch

	// Connection natural trigger.
	TimerDispatch       bool // reached through onTimerOp's expire dispatch
	ServerSide          bool
	HandlerPresent      bool
	HasArm              bool
	CurrentArmSeq       uint64
	ArmAfter            time.Duration // actual duration passed to the timer at arm time
	SinceArm            time.Duration // monotonic elapsed since that arm, at this trigger
	ArmedStateUnchanged bool          // stored exp and nextExpire still equal the arm snapshot
	MonotonicDue        bool          // SinceArm >= ArmAfter
	ExpiryReachedObs    bool          // observer-clock arithmetic: observed now >= PriorExpire

	// Subscription natural trigger (branch's own values; no second clock read).
	PredicateStrictGT       bool  // nowUnix > expireAt + DelaySeconds, as evaluated by the branch
	DelaySeconds            int64 // int64(delay.Seconds()) used by the branch
	CopiedStateStillCurrent bool  // copied expireAt equals the current map entry
	SubscriptionPresent     bool

	DroppedBefore uint64 // events this client dropped before this one
}

type goodnotesArm struct {
	valid      bool
	seq        uint64
	after      time.Duration
	mono       time.Time
	exp        int64
	nextExpire int64
}

type goodnotesTimingState struct {
	sink     atomic.Pointer[chan<- GoodnotesTimingEvent]
	causeSeq atomic.Uint64
	dropped  atomic.Uint64
	// Guarded by Client.mu.
	stateSeq    uint64
	stateOrigin GoodnotesStateOrigin
	armSeq      uint64
	arm         goodnotesArm
}

// SetGoodnotesTimingObserver installs a send-only sink for this client (nil keeps it off) and
// records the currently stored expiration as GoodnotesStateObserved. Call it from OnConnect.
func (c *Client) SetGoodnotesTimingObserver(sink chan<- GoodnotesTimingEvent) {
	if sink == nil {
		return
	}
	c.gn.sink.Store(&sink)
	c.mu.RLock()
	ev := GoodnotesTimingEvent{
		Kind: GoodnotesStateObserved, StateSeq: c.gn.stateSeq, Origin: c.gn.stateOrigin, PriorExpire: c.exp,
	}
	c.mu.RUnlock()
	c.gnSend(ev)
}

func (c *Client) gnEnabled() bool { return c.gn.sink.Load() != nil }

func (c *Client) gnSend(ev GoodnotesTimingEvent) {
	sink := c.gn.sink.Load()
	if sink == nil {
		return
	}
	defer func() {
		// e.g. a closed channel: lose the observation only, but count it as dropped so a later
		// healthy sink cannot hide the gap behind DroppedBefore == 0.
		if recover() != nil {
			c.gn.dropped.Add(1)
		}
	}()
	ev.ClientID = c.uid
	ev.DroppedBefore = c.gn.dropped.Load()
	select {
	case *sink <- ev:
	default:
		c.gn.dropped.Add(1)
	}
}

// Lock must be held outside. Sequence/origin are maintained even before a sink exists.
func (c *Client) gnRecordStateLocked(origin GoodnotesStateOrigin, prior, assigned int64, causeSeq uint64) {
	c.gn.stateSeq++
	c.gn.stateOrigin = origin
	if !c.gnEnabled() {
		return
	}
	c.gnSend(GoodnotesTimingEvent{Kind: GoodnotesStateRecorded, StateSeq: c.gn.stateSeq, Origin: origin,
		CauseSeq: causeSeq, PriorExpire: prior, NewExpire: assigned})
}

// Lock must be held outside. Records the actual expire-timer arm arguments, unchanged.
func (c *Client) gnArmLocked(after time.Duration) {
	if !c.gnEnabled() {
		return
	}
	c.gn.armSeq++
	c.gn.arm = goodnotesArm{valid: true, seq: c.gn.armSeq, after: after, mono: time.Now(), exp: c.exp, nextExpire: c.nextExpire}
}

type goodnotesConnSnapshot struct {
	enabled     bool
	stateSeq    uint64
	stateOrigin GoodnotesStateOrigin
	arm         goodnotesArm
	nextExpire  int64
}

// Lock (read) must be held outside.
func (c *Client) gnConnSnapshotLocked() goodnotesConnSnapshot {
	if !c.gnEnabled() {
		return goodnotesConnSnapshot{}
	}
	return goodnotesConnSnapshot{enabled: true, stateSeq: c.gn.stateSeq, stateOrigin: c.gn.stateOrigin,
		arm: c.gn.arm, nextExpire: c.nextExpire}
}

// gnConnTrigger emits the natural trigger before the handler runs and returns its cause sequence.
func (c *Client) gnConnTrigger(s goodnotesConnSnapshot, exp int64, timerDispatch, serverSide, handlerPresent bool) uint64 {
	if !s.enabled {
		return 0
	}
	causeSeq := c.gn.causeSeq.Add(1)
	ev := GoodnotesTimingEvent{Kind: GoodnotesNaturalTrigger, StateSeq: s.stateSeq, Origin: s.stateOrigin,
		CauseSeq: causeSeq, PriorExpire: exp, TimerDispatch: timerDispatch, ServerSide: serverSide,
		HandlerPresent: handlerPresent}
	observed := time.Now() // Labelled observer clock: expire() itself has no expiry predicate.
	ev.ExpiryReachedObs = exp > 0 && observed.Unix() >= exp
	if s.arm.valid {
		ev.HasArm = true
		ev.CurrentArmSeq = s.arm.seq
		ev.ArmAfter = s.arm.after
		ev.SinceArm = observed.Sub(s.arm.mono)
		ev.ArmedStateUnchanged = s.arm.exp == exp && s.arm.nextExpire == s.nextExpire
		ev.MonotonicDue = ev.SinceArm >= s.arm.after
	}
	c.gnSend(ev)
	return causeSeq
}

func (c *Client) gnConnNotApplied(causeSeq uint64, prior int64, branch GoodnotesNotAppliedBranch) {
	if causeSeq == 0 {
		return
	}
	c.gnSend(GoodnotesTimingEvent{Kind: GoodnotesNotApplied, CauseSeq: causeSeq, PriorExpire: prior, Branch: branch})
}

// gnSubTrigger emits the presence-check expired branch with the branch's own values.
func (c *Client) gnSubTrigger(channel string, copiedExpireAt, delaySeconds int64, serverSide, handlerPresent bool) uint64 {
	if !c.gnEnabled() {
		return 0
	}
	c.mu.RLock()
	current, present := c.channels[channel]
	c.mu.RUnlock()
	causeSeq := c.gn.causeSeq.Add(1)
	c.gnSend(GoodnotesTimingEvent{Kind: GoodnotesSubNaturalTrigger, Channel: channel, CauseSeq: causeSeq,
		PriorExpire: copiedExpireAt, PredicateStrictGT: true, DelaySeconds: delaySeconds,
		ServerSide: serverSide, HandlerPresent: handlerPresent, SubscriptionPresent: present,
		CopiedStateStillCurrent: present && current.expireAt == copiedExpireAt})
	return causeSeq
}

// Lock may be held outside (send is non-blocking).
func (c *Client) gnSubResult(kind GoodnotesTimingKind, causeSeq uint64, channel string, prior, assigned int64, branch GoodnotesNotAppliedBranch) {
	if causeSeq == 0 {
		return
	}
	c.gnSend(GoodnotesTimingEvent{Kind: kind, Channel: channel, CauseSeq: causeSeq, PriorExpire: prior,
		NewExpire: assigned, Branch: branch})
}
