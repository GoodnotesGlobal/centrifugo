package client

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/proxy"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// Production format: internal/logging.Setup does `log.Logger = log.Output(writer)` on zerolog's
// global logger, which carries the default `With().Timestamp()` context. With no log.file and
// stdout not a TTY, writer is os.Stderr and every line is JSON with an extra "time" field.
// (A TTY switches to zerolog.ConsoleWriter, which is NOT JSON; log.file writes JSON to a file.)
//
// So a witness line is: the witness's own fixed fields, plus exactly the known envelope "time".
// "level" and "message" are already part of each witness's own field set.
var productionEnvelope = []string{"time"}

func productionFormatLines(t *testing.T, emit func()) []map[string]any {
	t.Helper()
	out := &lockedBuffer{}
	production := log.Output(out) // the exact production expression, captured writer
	prevEnabled, prevLogger := refreshWitnessEnabled, refreshWitnessLogger
	refreshWitnessEnabled = true
	refreshWitnessLogger = func() *zerolog.Logger { return &production }
	t.Cleanup(func() { refreshWitnessEnabled, refreshWitnessLogger = prevEnabled, prevLogger })
	emit()
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(out.raw()), "\n") {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &m), "production mode is one JSON object per line")
		lines = append(lines, m)
	}
	return lines
}

func ownPlusEnvelope(own []string) []string {
	keys := append(append([]string{}, own...), productionEnvelope...)
	sort.Strings(keys)
	return keys
}

func TestGoodnotesWitnessLinesInProductionLoggerFormat(t *testing.T) {
	appliedOwn := []string{"applied_equals_reply", "applied_expiry_sha256", "build", "closed", "k", "level",
		"message", "outcome", "reply_expiry_sha256", "witness"}
	subAppliedOwn := []string{"applied_equals_reply", "applied_expiry_sha256", "build", "k", "level", "message",
		"outcome", "reply_expiry_sha256", "subscribed", "witness"}
	lines := productionFormatLines(t, func() {
		emitRefreshApplied(fakeAppliedState{id: "private-client", exp: 1824536000},
			centrifuge.RefreshReply{ExpireAt: 1824536000}, proxy.RefreshOutcomeBackendExpireAt)
		emitSubRefreshApplied(fakeSubscriptionState{id: "private-client", channel: "ch", exp: 1793000020, subscribed: true},
			"ch", centrifuge.SubRefreshReply{ExpireAt: 1793000020}, proxy.RefreshOutcomeBackendExpireAt)
		emitRefreshTiming(centrifuge.GoodnotesTimingEvent{Kind: centrifuge.GoodnotesNaturalTrigger, ClientID: "private-client",
			CauseSeq: 1, PriorExpire: 1793000000, TimerDispatch: true})
	})
	require.Len(t, lines, 3)
	require.Equal(t, ownPlusEnvelope(appliedOwn), keysOf(lines[0]))
	require.Equal(t, ownPlusEnvelope(subAppliedOwn), keysOf(lines[1]))
	require.Equal(t, ownPlusEnvelope(timingKeys), keysOf(lines[2]))
	for _, l := range lines {
		ts, ok := l["time"].(string)
		require.True(t, ok)
		_, err := time.Parse(zerolog.TimeFieldFormat, ts)
		require.NoError(t, err, "envelope time uses zerolog.TimeFieldFormat")
		require.Equal(t, "info", l["level"])
		raw, _ := json.Marshal(l)
		require.NotContains(t, string(raw), "private-client")
	}
}
