package client

import (
	"bytes"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/proxy"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type fakeAppliedState struct {
	id      string
	exp     int64
	closed  bool
	panicky bool
}

func (f fakeAppliedState) ID() string { return f.id }

func (f fakeAppliedState) AppliedRefreshState() (int64, bool) {
	if f.panicky {
		panic("broken state reader")
	}
	return f.exp, f.closed
}

func captureRefreshWitness(t *testing.T, enabled bool) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := zerolog.New(buf)
	prevEnabled, prevLogger := refreshWitnessEnabled, refreshWitnessLogger
	refreshWitnessEnabled = enabled
	refreshWitnessLogger = func() *zerolog.Logger { return &logger }
	t.Cleanup(func() { refreshWitnessEnabled, refreshWitnessLogger = prevEnabled, prevLogger })
	return buf
}

func TestGoodnotesRefreshWitnessVectorsMatchSharedFile(t *testing.T) {
	raw, err := os.ReadFile("testdata_goodnotes_refresh_witness_vectors_v1.json")
	require.NoError(t, err)
	var doc struct {
		KDomain      string `json:"kDomain"`
		ExpiryDomain string `json:"expiryDomain"`
		Vectors      []struct {
			Client       string `json:"client"`
			ExpireAt     int64  `json:"expireAt"`
			K            string `json:"k"`
			ExpiryCommit string `json:"expiryCommit"`
		} `json:"vectors"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.Equal(t, string(refreshKDomain), doc.KDomain)
	require.Equal(t, string(refreshExpiryDomain), doc.ExpiryDomain)
	require.Len(t, doc.Vectors, 3)
	for _, v := range doc.Vectors {
		require.Equal(t, v.K, refreshCorrelationSHA256(v.Client))
		require.Equal(t, v.ExpiryCommit, refreshExpirySHA256(v.Client, v.ExpireAt))
	}
	require.Equal(t, "", refreshExpirySHA256("c", 0))
	require.Equal(t, "", refreshExpirySHA256("c", -1))
	require.NotEqual(t, refreshExpirySHA256("c1", 23), refreshExpirySHA256("c", 123))
}

func TestGoodnotesRefreshWitnessAppliedLineIsClosedAndContentFree(t *testing.T) {
	buf := captureRefreshWitness(t, true)
	client := "private-client-id"
	emitRefreshApplied(fakeAppliedState{id: client, exp: 1824536000},
		centrifuge.RefreshReply{ExpireAt: 1824536000, Info: []byte("private-info")}, proxy.RefreshOutcomeBackendExpireAt)
	line := strings.TrimSpace(buf.String())
	require.NotContains(t, line, client)
	require.NotContains(t, line, "private-info")
	require.NotContains(t, line, "1824536000")
	var fields map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &fields))
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	require.Equal(t, []string{"applied_equals_reply", "applied_expiry_sha256", "build", "closed", "k", "level",
		"message", "outcome", "reply_expiry_sha256", "witness"}, keys)
	require.Equal(t, "goodnotes refresh applied", fields["message"])
	require.Equal(t, "goodnotes.refresh-applied.v1", fields["witness"])
	require.Equal(t, refreshCorrelationSHA256(client), fields["k"])
	require.Equal(t, "backendExpireAt", fields["outcome"])
	require.Equal(t, refreshExpirySHA256(client, 1824536000), fields["reply_expiry_sha256"])
	require.Equal(t, fields["reply_expiry_sha256"], fields["applied_expiry_sha256"])
	require.Equal(t, true, fields["applied_equals_reply"])
	require.Equal(t, false, fields["closed"])
}

func TestGoodnotesRefreshWitnessDistinguishesGraceClampAndClose(t *testing.T) {
	buf := captureRefreshWitness(t, true)
	emitRefreshApplied(fakeAppliedState{id: "a", exp: 1700000060}, centrifuge.RefreshReply{ExpireAt: 1700000060}, proxy.RefreshOutcomeProxyErrorGrace60)
	emitRefreshApplied(fakeAppliedState{id: "b", exp: 1700000000}, centrifuge.RefreshReply{ExpireAt: 1900000000}, proxy.RefreshOutcomeBackendExpireAt)
	emitRefreshApplied(fakeAppliedState{id: "c", exp: 1700000000, closed: true}, centrifuge.RefreshReply{Expired: true}, proxy.RefreshOutcomeNoResult)
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &m))
		lines = append(lines, m)
	}
	require.Len(t, lines, 3)
	require.Equal(t, "proxyErrorGrace60", lines[0]["outcome"])
	require.Equal(t, false, lines[1]["applied_equals_reply"]) // e.g. a clamp: fails closed
	require.Equal(t, true, lines[2]["closed"])
	require.Equal(t, "", lines[2]["applied_expiry_sha256"])
	require.Equal(t, "", lines[2]["reply_expiry_sha256"])
}

func TestGoodnotesRefreshWitnessDisabledTokenPathAndPanicsEmitNothing(t *testing.T) {
	buf := captureRefreshWitness(t, false)
	emitRefreshApplied(fakeAppliedState{id: "a", exp: 1}, centrifuge.RefreshReply{ExpireAt: 1}, proxy.RefreshOutcomeBackendExpireAt)
	require.Empty(t, buf.String())
	buf = captureRefreshWitness(t, true)
	emitRefreshApplied(fakeAppliedState{id: "a", exp: 1}, centrifuge.RefreshReply{ExpireAt: 1}, "")
	require.Empty(t, buf.String())
	require.NotPanics(t, func() {
		emitRefreshApplied(fakeAppliedState{id: "a", panicky: true}, centrifuge.RefreshReply{ExpireAt: 1}, proxy.RefreshOutcomeBackendExpireAt)
	})
	require.Empty(t, buf.String())
}

func TestGoodnotesRefreshWitnessEnvDefaultIsOff(t *testing.T) {
	require.NotEqual(t, "true", os.Getenv(refreshWitnessEnv))
	require.False(t, refreshWitnessEnabled)
}

type fakeSubscriptionState struct {
	id         string
	channel    string
	exp        int64
	subscribed bool
	panicky    bool
}

func (f fakeSubscriptionState) ID() string { return f.id }

func (f fakeSubscriptionState) AppliedSubscriptionState(channel string) (int64, bool) {
	if f.panicky {
		panic("broken state reader")
	}
	if channel != f.channel {
		return 0, false
	}
	return f.exp, f.subscribed
}

func TestGoodnotesSubRefreshWitnessVectorsMatchSharedFile(t *testing.T) {
	raw, err := os.ReadFile("testdata_goodnotes_refresh_witness_vectors_v1.json")
	require.NoError(t, err)
	var doc struct {
		KDomain      string `json:"subRefreshKDomain"`
		ExpiryDomain string `json:"subRefreshExpiryDomain"`
		Vectors      []struct {
			Client       string `json:"client"`
			Channel      string `json:"channel"`
			ExpireAt     int64  `json:"expireAt"`
			K            string `json:"k"`
			ExpiryCommit string `json:"expiryCommit"`
		} `json:"subRefreshVectors"`
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.Equal(t, string(subRefreshKDomain), doc.KDomain)
	require.Equal(t, string(subRefreshExpiryDomain), doc.ExpiryDomain)
	require.Len(t, doc.Vectors, 2)
	for _, v := range doc.Vectors {
		require.Equal(t, v.K, subRefreshCorrelationSHA256(v.Client, v.Channel))
		require.Equal(t, v.ExpiryCommit, subRefreshExpirySHA256(v.Client, v.Channel, v.ExpireAt))
	}
	require.NotEqual(t, subRefreshCorrelationSHA256("a", "bc"), subRefreshCorrelationSHA256("ab", "c"))
	require.NotEqual(t, subRefreshCorrelationSHA256("c", "x"), refreshCorrelationSHA256("c"))
}

func TestGoodnotesSubRefreshWitnessAppliedRefusedAndSafety(t *testing.T) {
	buf := captureRefreshWitness(t, true)
	client, channel := "private-client-id", "textdoc-presence:private-doc"
	emitSubRefreshApplied(fakeSubscriptionState{id: client, channel: channel, exp: 1793000020, subscribed: true},
		channel, centrifuge.SubRefreshReply{ExpireAt: 1793000020, Info: []byte("private-info")}, proxy.RefreshOutcomeBackendExpireAt)
	// Refusal: removal is asynchronous, so still-subscribed is recorded but never a pass.
	emitSubRefreshApplied(fakeSubscriptionState{id: client, channel: channel, exp: 1793000000, subscribed: true},
		channel, centrifuge.SubRefreshReply{Expired: true}, proxy.RefreshOutcomeBackendExpired)
	out := buf.String()
	for _, secret := range []string{client, channel, "private-doc", "private-info", "1793000020"} {
		require.NotContains(t, out, secret)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2)
	var applied, refused map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &applied))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &refused))
	keys := make([]string, 0, len(applied))
	for k := range applied {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	require.Equal(t, []string{"applied_equals_reply", "applied_expiry_sha256", "build", "k", "level", "message",
		"outcome", "reply_expiry_sha256", "subscribed", "witness"}, keys)
	require.Equal(t, "goodnotes.sub-refresh-applied.v1", applied["witness"])
	require.Equal(t, subRefreshCorrelationSHA256(client, channel), applied["k"])
	require.Equal(t, subRefreshExpirySHA256(client, channel, 1793000020), applied["applied_expiry_sha256"])
	require.Equal(t, true, applied["applied_equals_reply"])
	require.Equal(t, "backendExpired", refused["outcome"])
	require.Equal(t, "", refused["reply_expiry_sha256"])
	require.Equal(t, false, refused["applied_equals_reply"])

	buf.Reset()
	require.NotPanics(t, func() {
		emitSubRefreshApplied(fakeSubscriptionState{panicky: true}, channel, centrifuge.SubRefreshReply{ExpireAt: 1}, proxy.RefreshOutcomeBackendExpireAt)
	})
	emitSubRefreshApplied(fakeSubscriptionState{id: client, channel: channel, subscribed: true}, channel, centrifuge.SubRefreshReply{ExpireAt: 1}, "")
	require.Empty(t, buf.String())
	buf = captureRefreshWitness(t, false)
	emitSubRefreshApplied(fakeSubscriptionState{id: client, channel: channel, subscribed: true}, channel, centrifuge.SubRefreshReply{ExpireAt: 1}, proxy.RefreshOutcomeBackendExpireAt)
	require.Empty(t, buf.String())
}
