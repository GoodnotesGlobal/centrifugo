package centrifuge

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func appliedStateTestClient(t *testing.T, reply RefreshReply, err error) (*Client, int64) {
	t.Helper()
	node := defaultTestNode()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	initial := time.Now().Unix() + 60
	node.OnConnect(func(client *Client) {
		client.OnRefresh(func(_ RefreshEvent, cb RefreshCallback) { cb(reply, err) })
	})
	ctx := SetCredentials(context.Background(), &Credentials{UserID: "42", ExpireAt: initial})
	client, _ := newClient(ctx, node, newTestTransport(func() {}))
	rw := testReplyWriterWrapper()
	require.NoError(t, client.connectCmd(&protocol.ConnectRequest{}, &protocol.Command{}, time.Now(), rw.rw))
	client.triggerConnect()
	return client, initial
}

func TestAppliedRefreshStateReflectsConnectExpiration(t *testing.T) {
	client, initial := appliedStateTestClient(t, RefreshReply{}, nil)
	exp, closed := client.AppliedRefreshState()
	require.Equal(t, initial, exp)
	require.False(t, closed)
}

func TestAppliedRefreshStateAfterServerSideRefreshApplied(t *testing.T) {
	renewed := time.Now().Unix() + 600
	client, _ := appliedStateTestClient(t, RefreshReply{ExpireAt: renewed}, nil)
	client.expire()
	exp, closed := client.AppliedRefreshState()
	require.Equal(t, renewed, exp)
	require.False(t, closed)
	require.Equal(t, client.exp, exp)
}

func TestAppliedRefreshStateAfterExpiredRefreshIsClosed(t *testing.T) {
	client, _ := appliedStateTestClient(t, RefreshReply{Expired: true}, nil)
	client.expire()
	_, closed := client.AppliedRefreshState()
	require.True(t, closed)
}

func TestAppliedRefreshStateAfterHandlerErrorIsClosed(t *testing.T) {
	client, _ := appliedStateTestClient(t, RefreshReply{}, DisconnectServerError)
	client.expire()
	_, closed := client.AppliedRefreshState()
	require.True(t, closed)
}

func TestAppliedSubscriptionStateReadsTheRenewedChannelExpiration(t *testing.T) {
	node := defaultTestNode()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	client := newTestClient(t, node, "42")
	var nowTime time.Time
	node.mu.Lock()
	node.nowTimeGetter = func() time.Time { return nowTime }
	node.mu.Unlock()

	_, subscribed := client.AppliedSubscriptionState("channel")
	require.False(t, subscribed)

	client.channels["channel"] = ChannelContext{expireAt: 100}
	exp, subscribed := client.AppliedSubscriptionState("channel")
	require.Equal(t, int64(100), exp)
	require.True(t, subscribed)

	client.eventHub.subRefreshHandler = func(_ SubRefreshEvent, cb SubRefreshCallback) {
		cb(SubRefreshReply{ExpireAt: 250}, nil)
	}
	nowTime = time.Unix(200, 0)
	renewed := false
	client.checkSubscriptionExpiration("channel", ChannelContext{expireAt: 100}, 50*time.Second, func(b bool) { renewed = b })
	require.True(t, renewed)
	exp, subscribed = client.AppliedSubscriptionState("channel")
	require.Equal(t, int64(250), exp)
	require.True(t, subscribed)

	// A refusal leaves the map untouched here; removal is asynchronous in updatePresence.
	client.eventHub.subRefreshHandler = func(_ SubRefreshEvent, cb SubRefreshCallback) {
		cb(SubRefreshReply{Expired: true}, nil)
	}
	client.checkSubscriptionExpiration("channel", ChannelContext{expireAt: 100}, 50*time.Second, func(b bool) { renewed = b })
	require.False(t, renewed)
	exp, subscribed = client.AppliedSubscriptionState("channel")
	require.Equal(t, int64(250), exp)
	require.True(t, subscribed)
}
