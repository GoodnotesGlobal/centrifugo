package centrifuge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func gnCollect(t *testing.T, ch <-chan GoodnotesTimingEvent, until func([]GoodnotesTimingEvent) bool) []GoodnotesTimingEvent {
	t.Helper()
	var got []GoodnotesTimingEvent
	deadline := time.After(6 * time.Second)
	for !until(got) {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case <-deadline:
			t.Fatalf("timeout; got %d events: %+v", len(got), got)
		}
	}
	return got
}

func gnHasKind(kinds ...GoodnotesTimingKind) func([]GoodnotesTimingEvent) bool {
	return func(evs []GoodnotesTimingEvent) bool {
		for _, ev := range evs {
			if ev.Kind == kinds[len(kinds)-1] {
				return true
			}
		}
		return false
	}
}

func gnFind(evs []GoodnotesTimingEvent, kind GoodnotesTimingKind) GoodnotesTimingEvent {
	for _, ev := range evs {
		if ev.Kind == kind {
			return ev
		}
	}
	return GoodnotesTimingEvent{}
}

// connectWithSink connects a server-side-refresh client whose credentials expire in ttl seconds,
// installing the sink in OnConnect (before scheduleOnConnectTimers), and lets the real timer run.
func connectWithSink(t *testing.T, ttl int64, sink chan GoodnotesTimingEvent, refresh func(cb RefreshCallback)) (*Client, int64) {
	t.Helper()
	node := defaultTestNode()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnect(func(client *Client) {
		client.SetGoodnotesTimingObserver(sink)
		client.OnRefresh(func(_ RefreshEvent, cb RefreshCallback) { refresh(cb) })
	})
	initial := time.Now().Unix() + ttl
	ctx := SetCredentials(context.Background(), &Credentials{UserID: "42", ExpireAt: initial})
	client, _ := newClient(ctx, node, newTestTransport(func() {}))
	connectClientV2(t, client)
	return client, initial
}

func TestGoodnotesTimingNaturalChainThroughRealTimer(t *testing.T) {
	sink := make(chan GoodnotesTimingEvent, 64)
	renewed := time.Now().Unix() + 600
	client, initial := connectWithSink(t, 1, sink, func(cb RefreshCallback) { cb(RefreshReply{ExpireAt: renewed}, nil) })
	evs := gnCollect(t, sink, gnHasKind(GoodnotesStateRecorded))

	observed := gnFind(evs, GoodnotesStateObserved)
	require.Equal(t, initial, observed.PriorExpire)
	require.Equal(t, GoodnotesOriginConnect, observed.Origin)
	require.Equal(t, uint64(1), observed.StateSeq)

	trig := gnFind(evs, GoodnotesNaturalTrigger)
	require.True(t, trig.TimerDispatch, "reached through onTimerOp expire dispatch")
	require.True(t, trig.ServerSide)
	require.True(t, trig.HandlerPresent)
	require.Equal(t, initial, trig.PriorExpire)
	require.Equal(t, observed.StateSeq, trig.StateSeq)
	require.Equal(t, GoodnotesOriginConnect, trig.Origin)
	require.True(t, trig.HasArm)
	require.NotZero(t, trig.CurrentArmSeq)
	require.True(t, trig.ArmedStateUnchanged)
	require.True(t, trig.MonotonicDue)
	require.GreaterOrEqual(t, trig.SinceArm, trig.ArmAfter)
	require.True(t, trig.ExpiryReachedObs)
	require.NotZero(t, trig.CauseSeq)

	applied := gnFind(evs, GoodnotesStateRecorded)
	require.Equal(t, GoodnotesOriginNaturalServerRefresh, applied.Origin)
	require.Equal(t, trig.CauseSeq, applied.CauseSeq)
	require.Equal(t, initial, applied.PriorExpire)
	require.Equal(t, renewed, applied.NewExpire)
	require.Equal(t, trig.StateSeq+1, applied.StateSeq)
	require.Equal(t, client.uid, applied.ClientID)
	require.Zero(t, applied.DroppedBefore)

	client.mu.RLock()
	require.Equal(t, renewed, client.exp)
	client.mu.RUnlock()
}

func TestGoodnotesTimingNotAppliedBranchesKeepUpstreamBehaviour(t *testing.T) {
	cases := map[string]struct {
		refresh func(cb RefreshCallback)
		branch  GoodnotesNotAppliedBranch
	}{
		"expired":    {func(cb RefreshCallback) { cb(RefreshReply{Expired: true}, nil) }, GoodnotesBranchExpired},
		"disconnect": {func(cb RefreshCallback) { cb(RefreshReply{}, DisconnectServerError) }, GoodnotesBranchDisconnect},
		"error":      {func(cb RefreshCallback) { cb(RefreshReply{}, errors.New("boom")) }, GoodnotesBranchHandlerError},
		"zero":       {func(cb RefreshCallback) { cb(RefreshReply{}, nil) }, GoodnotesBranchZeroExpiration},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sink := make(chan GoodnotesTimingEvent, 64)
			client, _ := connectWithSink(t, 1, sink, tc.refresh)
			evs := gnCollect(t, sink, gnHasKind(GoodnotesNotApplied))
			trig := gnFind(evs, GoodnotesNaturalTrigger)
			na := gnFind(evs, GoodnotesNotApplied)
			require.Equal(t, trig.CauseSeq, na.CauseSeq)
			require.Equal(t, tc.branch, na.Branch)
			require.Equal(t, GoodnotesTimingEvent{}, gnFind(evs, GoodnotesStateRecorded))
			require.Eventually(t, func() bool {
				client.mu.RLock()
				defer client.mu.RUnlock()
				return client.status == statusClosed
			}, 2*time.Second, 10*time.Millisecond)
		})
	}
}

func gnManualClient(t *testing.T, exp int64, sink chan GoodnotesTimingEvent, reply RefreshReply) *Client {
	t.Helper()
	node := defaultTestNode()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(_ RefreshEvent, cb RefreshCallback) { cb(reply, nil) })
	})
	ctx := SetCredentials(context.Background(), &Credentials{UserID: "42", ExpireAt: exp})
	client, _ := newClient(ctx, node, newTestTransport(func() {}))
	rw := testReplyWriterWrapper()
	require.NoError(t, client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw))
	client.triggerConnect()
	client.SetGoodnotesTimingObserver(sink)
	return client
}

func TestGoodnotesTimingEarlyOverwrittenAndNonTimerAreDistinguishable(t *testing.T) {
	sink := make(chan GoodnotesTimingEvent, 64)
	exp := time.Now().Unix() + 30
	client := gnManualClient(t, exp, sink, RefreshReply{ExpireAt: exp + 600})
	client.mu.Lock()
	client.addExpireUpdate(30*time.Second, true) // the real arm path
	client.mu.Unlock()

	// Early dispatch: same arm, called immediately.
	client.expireFrom(true)
	early := gnFind(gnCollect(t, sink, gnHasKind(GoodnotesStateRecorded)), GoodnotesNaturalTrigger)
	require.True(t, early.HasArm)
	require.True(t, early.ArmedStateUnchanged)
	require.False(t, early.MonotonicDue)
	require.False(t, early.ExpiryReachedObs)

	// The apply re-armed from the new expiration. Now change the stored expiration without a
	// re-arm (an overwritten state): the current arm snapshot no longer matches.
	client.mu.Lock()
	client.exp += 5
	client.mu.Unlock()
	client.expireFrom(true)
	stale := gnFind(gnCollect(t, sink, gnHasKind(GoodnotesNaturalTrigger)), GoodnotesNaturalTrigger)
	require.False(t, stale.ArmedStateUnchanged)

	// Not through onTimerOp.
	client.expire()
	direct := gnFind(gnCollect(t, sink, gnHasKind(GoodnotesNaturalTrigger)), GoodnotesNaturalTrigger)
	require.False(t, direct.TimerDispatch)
}

func TestGoodnotesTimingOriginsStayDistinct(t *testing.T) {
	sink := make(chan GoodnotesTimingEvent, 64)
	exp := time.Now().Unix() + 30
	client := gnManualClient(t, exp, sink, RefreshReply{})
	gnCollect(t, sink, gnHasKind(GoodnotesStateObserved))

	require.NoError(t, client.Refresh(WithRefreshExpireAt(exp+100)))
	api := gnFind(gnCollect(t, sink, gnHasKind(GoodnotesStateRecorded)), GoodnotesStateRecorded)
	require.Equal(t, GoodnotesOriginServerAPI, api.Origin)
	require.Zero(t, api.CauseSeq)
	require.Equal(t, exp, api.PriorExpire)
	require.Equal(t, exp+100, api.NewExpire)

	require.NoError(t, client.Refresh(WithRefreshExpireAt(0)))
	cleared := gnFind(gnCollect(t, sink, gnHasKind(GoodnotesStateRecorded)), GoodnotesStateRecorded)
	require.Equal(t, GoodnotesOriginServerAPI, cleared.Origin)
	require.Zero(t, cleared.NewExpire)
	require.Equal(t, api.StateSeq+1, cleared.StateSeq)
}

func TestGoodnotesTimingSubscriptionStrictPredicateAndCopiedState(t *testing.T) {
	node := defaultTestNode()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	client := newTestClient(t, node, "42")
	sink := make(chan GoodnotesTimingEvent, 64)
	client.SetGoodnotesTimingObserver(sink)
	var nowTime time.Time
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time { return nowTime }
	node.mu.Unlock()
	client.eventHub.subRefreshHandler = func(_ SubRefreshEvent, cb SubRefreshCallback) { cb(SubRefreshReply{ExpireAt: 400}, nil) }
	client.channels["ch"] = ChannelContext{expireAt: 100}
	copied := ChannelContext{expireAt: 100}

	// Equality at the threshold does not trigger (strict >).
	nowTime = time.Unix(150, 0)
	client.checkSubscriptionExpiration("ch", copied, 50*time.Second, func(bool) {})
	select {
	case ev := <-sink:
		require.Equal(t, GoodnotesStateObserved, ev.Kind)
	default:
	}
	select {
	case ev := <-sink:
		t.Fatalf("unexpected event at threshold: %+v", ev)
	default:
	}

	nowTime = time.Unix(151, 0)
	client.checkSubscriptionExpiration("ch", copied, 50*time.Second, func(b bool) { require.True(t, b) })
	evs := gnCollect(t, sink, gnHasKind(GoodnotesSubStateApplied))
	trig := gnFind(evs, GoodnotesSubNaturalTrigger)
	require.True(t, trig.PredicateStrictGT)
	require.Equal(t, int64(50), trig.DelaySeconds)
	require.Equal(t, int64(100), trig.PriorExpire)
	require.True(t, trig.CopiedStateStillCurrent)
	applied := gnFind(evs, GoodnotesSubStateApplied)
	require.Equal(t, trig.CauseSeq, applied.CauseSeq)
	require.Equal(t, int64(100), applied.PriorExpire)
	require.Equal(t, int64(400), applied.NewExpire)
	require.Equal(t, "ch", applied.Channel)

	// The decision used a stale copy (map already renewed to 400).
	nowTime = time.Unix(151, 0)
	client.checkSubscriptionExpiration("ch", copied, 50*time.Second, func(bool) {})
	stale := gnFind(gnCollect(t, sink, gnHasKind(GoodnotesSubNaturalTrigger)), GoodnotesSubNaturalTrigger)
	require.False(t, stale.CopiedStateStillCurrent)

	// Missing channel: upstream still reports true; the witness records not-applied.
	delete(client.channels, "ch")
	client.checkSubscriptionExpiration("ch", copied, 50*time.Second, func(b bool) { require.True(t, b) })
	evs = gnCollect(t, sink, gnHasKind(GoodnotesSubNotApplied))
	require.Equal(t, GoodnotesBranchChannelMissing, gnFind(evs, GoodnotesSubNotApplied).Branch)
	require.False(t, gnFind(evs, GoodnotesSubNaturalTrigger).SubscriptionPresent)

	// Refused renewal: recorded, removal never claimed here.
	client.channels["ch"] = ChannelContext{expireAt: 100}
	client.eventHub.subRefreshHandler = func(_ SubRefreshEvent, cb SubRefreshCallback) { cb(SubRefreshReply{Expired: true}, nil) }
	client.checkSubscriptionExpiration("ch", copied, 50*time.Second, func(b bool) { require.False(t, b) })
	evs = gnCollect(t, sink, gnHasKind(GoodnotesSubNotApplied))
	require.Equal(t, GoodnotesBranchExpired, gnFind(evs, GoodnotesSubNotApplied).Branch)
	require.EqualValues(t, 100, client.channels["ch"].expireAt)
}

func TestGoodnotesTimingSinkFullClosedOrNilNeverChangesBehaviour(t *testing.T) {
	for name, mk := range map[string]func() chan GoodnotesTimingEvent{
		"full":   func() chan GoodnotesTimingEvent { return make(chan GoodnotesTimingEvent) }, // unbuffered, nobody reads
		"closed": func() chan GoodnotesTimingEvent { ch := make(chan GoodnotesTimingEvent, 1); close(ch); return ch },
		"nil":    func() chan GoodnotesTimingEvent { return nil },
	} {
		t.Run(name, func(t *testing.T) {
			exp := time.Now().Unix() + 30
			client := gnManualClient(t, exp, mk(), RefreshReply{ExpireAt: exp + 600})
			require.NotPanics(t, func() { client.expireFrom(true) })
			client.mu.RLock()
			require.Equal(t, exp+600, client.exp)
			require.NotEqual(t, statusClosed, client.status)
			client.mu.RUnlock()
			switch name {
			case "full", "closed":
				require.NotZero(t, client.gn.dropped.Load(), "a lost observation is always counted")
			case "nil":
				require.Zero(t, client.gn.dropped.Load(), "no sink never fabricates drops")
			}
		})
	}
}

func TestGoodnotesTimingNoSinkKeepsStateSeqButEmitsNothing(t *testing.T) {
	exp := time.Now().Unix() + 30
	client := gnManualClient(t, exp, nil, RefreshReply{ExpireAt: exp + 600})
	client.expireFrom(true)
	client.mu.RLock()
	defer client.mu.RUnlock()
	require.Equal(t, exp+600, client.exp)
	require.Equal(t, uint64(2), client.gn.stateSeq) // connect + natural
	require.Equal(t, GoodnotesOriginNaturalServerRefresh, client.gn.stateOrigin)
	require.False(t, client.gn.arm.valid) // no clock read without a sink
}

func TestGoodnotesTimingClosedSinkDropsSurfaceThroughAReplacementSink(t *testing.T) {
	closed := make(chan GoodnotesTimingEvent, 1)
	close(closed)
	exp := time.Now().Unix() + 30
	client := gnManualClient(t, exp, closed, RefreshReply{ExpireAt: exp + 600})
	require.Equal(t, uint64(1), client.gn.dropped.Load(), "the state-observed send on the closed sink")
	client.expireFrom(true) // trigger + state-recorded, both lost on the closed sink
	lost := client.gn.dropped.Load()
	require.Equal(t, uint64(3), lost)

	healthy := make(chan GoodnotesTimingEvent, 16)
	client.SetGoodnotesTimingObserver(healthy)
	ev := <-healthy
	require.Equal(t, GoodnotesStateObserved, ev.Kind)
	require.Equal(t, lost, ev.DroppedBefore, "the gap stays visible to the next successful event")
	require.Equal(t, exp+600, ev.PriorExpire)
}
