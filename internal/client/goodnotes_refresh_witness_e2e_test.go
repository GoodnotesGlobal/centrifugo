package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
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

// End-to-end through the real Handler.Setup wiring, a real centrifuge.Client and the node's own
// expiration timer (no unexported centrifuge call): connect proxy grants a 1 s connection, the
// timer runs server-side refresh against a fake refresh proxy, the wrapper emits after cb.

type e2eConnectProxy struct{ expireAt int64 }

func (p e2eConnectProxy) ProxyConnect(context.Context, *proxyproto.ConnectRequest) (*proxyproto.ConnectResponse, error) {
	return &proxyproto.ConnectResponse{Result: &proxyproto.ConnectResult{User: "42", ExpireAt: p.expireAt}}, nil
}
func (e2eConnectProxy) Name() string     { return "default" }
func (e2eConnectProxy) Protocol() string { return "http" }
func (e2eConnectProxy) UseBase64() bool  { return false }

type e2eRefreshProxy struct {
	mu    sync.Mutex
	calls int
	reply func() (*proxyproto.RefreshResponse, error)
}

func (p *e2eRefreshProxy) ProxyRefresh(context.Context, *proxyproto.RefreshRequest) (*proxyproto.RefreshResponse, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return p.reply()
}
func (*e2eRefreshProxy) Name() string      { return "default" }
func (*e2eRefreshProxy) Protocol() string  { return "http" }
func (*e2eRefreshProxy) UseBase64() bool   { return false }
func (*e2eRefreshProxy) IncludeMeta() bool { return false }

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines() []map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["witness"] == "goodnotes.refresh-applied.v1" {
			out = append(out, m)
		}
	}
	return out
}

func (b *lockedBuffer) raw() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

var e2eMetricsOnce sync.Once

func runRefreshE2E(t *testing.T, reply func() (*proxyproto.RefreshResponse, error)) (*centrifuge.Client, *e2eRefreshProxy, map[string]any, string) {
	t.Helper()
	// Proxy handlers register Prometheus vectors; same initialisation as internal/proxy TestMain.
	e2eMetricsOnce.Do(func() { _ = metrics.Init(metrics.Config{Registerer: prometheus.NewRegistry()}) })
	out := &lockedBuffer{}
	logger := zerolog.New(out)
	prevEnabled, prevLogger := refreshWitnessEnabled, refreshWitnessLogger
	refreshWitnessEnabled = true
	refreshWitnessLogger = func() *zerolog.Logger { return &logger }
	t.Cleanup(func() { refreshWitnessEnabled, refreshWitnessLogger = prevEnabled, prevLogger })

	node := tools.NodeWithMemoryEngineNoHandlers()
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	cfgContainer, err := config.NewContainer(config.DefaultConfig())
	require.NoError(t, err)
	refreshProxy := &e2eRefreshProxy{reply: reply}
	h := NewHandler(node, cfgContainer, nil, nil, &ProxyMap{
		ConnectProxy: e2eConnectProxy{expireAt: time.Now().Unix() + 1},
		RefreshProxy: refreshProxy,
	})
	require.NoError(t, h.Setup())

	client, closeFn, err := centrifuge.NewClient(context.Background(), node, tools.NewTestTransport())
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	// Real bidirectional path (as for WebSocket): a protocol connect command.
	cmd := &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}
	require.True(t, client.HandleCommand(cmd, cmd.SizeVT()))
	initial, closed := client.AppliedRefreshState()
	require.False(t, closed)
	require.Greater(t, initial, int64(0))

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if lines := out.lines(); len(lines) > 0 {
			require.Len(t, lines, 1, "exactly one witness line for one server-side refresh")
			require.NotContains(t, out.raw(), client.ID())
			return client, refreshProxy, lines[0], client.ID()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no refresh-applied witness emitted by the node timer")
	return nil, nil, nil, ""
}

func TestGoodnotesRefreshWitnessE2EBackendExpiryApplied(t *testing.T) {
	backend := time.Now().Unix() + 600
	client, proxy, line, id := runRefreshE2E(t, func() (*proxyproto.RefreshResponse, error) {
		return &proxyproto.RefreshResponse{Result: &proxyproto.RefreshResult{ExpireAt: backend}}, nil
	})
	exp, closed := client.AppliedRefreshState()
	require.Equal(t, backend, exp)
	require.False(t, closed)
	require.Equal(t, 1, proxy.calls)
	require.Equal(t, "backendExpireAt", line["outcome"])
	require.Equal(t, refreshCorrelationSHA256(id), line["k"])
	require.Equal(t, refreshExpirySHA256(id, backend), line["reply_expiry_sha256"])
	require.Equal(t, refreshExpirySHA256(id, backend), line["applied_expiry_sha256"])
	require.Equal(t, true, line["applied_equals_reply"])
	require.Equal(t, false, line["closed"])
}

func TestGoodnotesRefreshWitnessE2EProxyErrorGraceIsTypedNotBackend(t *testing.T) {
	backendWouldHave := time.Now().Unix() + 600
	client, _, line, id := runRefreshE2E(t, func() (*proxyproto.RefreshResponse, error) {
		return nil, errors.New("backend unreachable")
	})
	exp, closed := client.AppliedRefreshState()
	require.False(t, closed)
	now := time.Now().Unix()
	require.GreaterOrEqual(t, exp, now+58)
	require.LessOrEqual(t, exp, now+60)
	require.Equal(t, "proxyErrorGrace60", line["outcome"])
	require.Equal(t, refreshExpirySHA256(id, exp), line["applied_expiry_sha256"])
	// The grace value never matches any Backend decision commitment.
	require.NotEqual(t, refreshExpirySHA256(id, backendWouldHave), line["applied_expiry_sha256"])
}

func TestGoodnotesRefreshWitnessE2EClampShowsMismatch(t *testing.T) {
	huge := time.Now().Unix() + 400*24*3600
	client, _, line, id := runRefreshE2E(t, func() (*proxyproto.RefreshResponse, error) {
		return &proxyproto.RefreshResponse{Result: &proxyproto.RefreshResult{ExpireAt: huge}}, nil
	})
	exp, closed := client.AppliedRefreshState()
	require.False(t, closed)
	require.Less(t, exp, huge)
	require.Equal(t, "backendExpireAt", line["outcome"])
	require.Equal(t, refreshExpirySHA256(id, huge), line["reply_expiry_sha256"])
	require.Equal(t, refreshExpirySHA256(id, exp), line["applied_expiry_sha256"])
	require.Equal(t, false, line["applied_equals_reply"])
}

func TestGoodnotesRefreshWitnessE2EExpiredAndDisconnectOnlyClose(t *testing.T) {
	for name, tc := range map[string]struct {
		resp    *proxyproto.RefreshResponse
		outcome string
	}{
		"expired":         {&proxyproto.RefreshResponse{Result: &proxyproto.RefreshResult{Expired: true}}, "backendExpired"},
		"disconnect-only": {&proxyproto.RefreshResponse{Disconnect: &proxyproto.Disconnect{Code: 4201, Reason: "session invalidated"}}, "noResult"},
	} {
		t.Run(name, func(t *testing.T) {
			client, _, line, _ := runRefreshE2E(t, func() (*proxyproto.RefreshResponse, error) { return tc.resp, nil })
			_, closed := client.AppliedRefreshState()
			require.True(t, closed)
			require.Equal(t, tc.outcome, line["outcome"])
			require.Equal(t, true, line["closed"])
			require.Equal(t, "", line["applied_expiry_sha256"])
			require.Equal(t, false, line["applied_equals_reply"])
		})
	}
}
