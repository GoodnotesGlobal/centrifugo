package client

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"strconv"

	"github.com/centrifugal/centrifugo/v6/internal/build"
	"github.com/centrifugal/centrifugo/v6/internal/proxy"

	"github.com/centrifugal/centrifuge"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Goodnotes refresh-applied witness v1.
//
// After a server-side refresh callback returns, read the connection state the node actually
// applies and emit one content-free line: a pseudonymous client correlation digest, the proxy
// branch, commitments to the reply's and the applied expiration, and whether the client closed.
// No user, info, meta, channel, token or payload is emitted. Off unless the env flag is "true".
// Emission happens after state is applied and is panic-guarded, so it cannot change behaviour.

const refreshWitnessEnv = "GOODNOTES_SPARSE_REFRESH_WITNESS_ENABLED"

var (
	refreshWitnessEnabled  = os.Getenv(refreshWitnessEnv) == "true"
	refreshWitnessLogger   = func() *zerolog.Logger { return &log.Logger }
	refreshKDomain         = []byte("goodnotes.centrifugo.refresh.v1\x00")
	refreshExpiryDomain    = []byte("goodnotes.centrifugo.refresh-expiry.v1\x00")
	subRefreshKDomain      = []byte("goodnotes.centrifugo.sub-refresh.v1\x00")
	subRefreshExpiryDomain = []byte("goodnotes.centrifugo.sub-refresh-expiry.v1\x00")
)

type appliedRefreshStateReader interface {
	ID() string
	AppliedRefreshState() (expireAt int64, closed bool)
}

var _ appliedRefreshStateReader = (*centrifuge.Client)(nil)

type appliedSubscriptionStateReader interface {
	ID() string
	AppliedSubscriptionState(channel string) (expireAt int64, subscribed bool)
}

var _ appliedSubscriptionStateReader = (*centrifuge.Client)(nil)

func refreshWitnessField(h interface{ Write([]byte) (int, error) }, value string) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(value))
}

func refreshCorrelationSHA256(client string) string {
	h := sha256.New()
	_, _ = h.Write(refreshKDomain)
	refreshWitnessField(h, client)
	return hex.EncodeToString(h.Sum(nil))
}

// refreshExpirySHA256 is defined only for a positive Unix time; otherwise it returns "".
func refreshExpirySHA256(client string, expireAt int64) string {
	if expireAt <= 0 {
		return ""
	}
	h := sha256.New()
	_, _ = h.Write(refreshExpiryDomain)
	refreshWitnessField(h, client)
	refreshWitnessField(h, strconv.FormatInt(expireAt, 10))
	return hex.EncodeToString(h.Sum(nil))
}

func emitRefreshApplied(c appliedRefreshStateReader, reply centrifuge.RefreshReply, outcome proxy.RefreshOutcome) {
	if !refreshWitnessEnabled || outcome == "" {
		return
	}
	defer func() { _ = recover() }()
	id := c.ID()
	applied, closed := c.AppliedRefreshState()
	replyCommit := refreshExpirySHA256(id, reply.ExpireAt)
	appliedCommit := ""
	if !closed {
		appliedCommit = refreshExpirySHA256(id, applied)
	}
	refreshWitnessLogger().Info().
		Str("witness", "goodnotes.refresh-applied.v1").
		Str("k", refreshCorrelationSHA256(id)).
		Str("outcome", string(outcome)).
		Str("reply_expiry_sha256", replyCommit).
		Str("applied_expiry_sha256", appliedCommit).
		Bool("applied_equals_reply", replyCommit != "" && replyCommit == appliedCommit).
		Bool("closed", closed).
		Str("build", build.Version).
		Msg("goodnotes refresh applied")
}

// subRefreshCorrelationSHA256 binds the client/channel pair; neither is emitted separately.
func subRefreshCorrelationSHA256(client, channel string) string {
	h := sha256.New()
	_, _ = h.Write(subRefreshKDomain)
	refreshWitnessField(h, client)
	refreshWitnessField(h, channel)
	return hex.EncodeToString(h.Sum(nil))
}

// subRefreshExpirySHA256 is defined only for a positive Unix time; otherwise it returns "".
func subRefreshExpirySHA256(client, channel string, expireAt int64) string {
	if expireAt <= 0 {
		return ""
	}
	h := sha256.New()
	_, _ = h.Write(subRefreshExpiryDomain)
	refreshWitnessField(h, client)
	refreshWitnessField(h, channel)
	refreshWitnessField(h, strconv.FormatInt(expireAt, 10))
	return hex.EncodeToString(h.Sum(nil))
}

// emitSubRefreshApplied reads the subscription state right after a server-side sub refresh
// callback returned. A renewal is applied synchronously; a refusal unsubscribes asynchronously,
// so for refused outcomes "subscribed" is recorded but proves nothing about removal.
func emitSubRefreshApplied(c appliedSubscriptionStateReader, channel string, reply centrifuge.SubRefreshReply, outcome proxy.RefreshOutcome) {
	if !refreshWitnessEnabled || outcome == "" {
		return
	}
	defer func() { _ = recover() }()
	id := c.ID()
	applied, subscribed := c.AppliedSubscriptionState(channel)
	replyCommit := ""
	if !reply.Expired {
		replyCommit = subRefreshExpirySHA256(id, channel, reply.ExpireAt)
	}
	appliedCommit := ""
	if subscribed {
		appliedCommit = subRefreshExpirySHA256(id, channel, applied)
	}
	refreshWitnessLogger().Info().
		Str("witness", "goodnotes.sub-refresh-applied.v1").
		Str("k", subRefreshCorrelationSHA256(id, channel)).
		Str("outcome", string(outcome)).
		Str("reply_expiry_sha256", replyCommit).
		Str("applied_expiry_sha256", appliedCommit).
		Bool("applied_equals_reply", replyCommit != "" && replyCommit == appliedCommit).
		Bool("subscribed", subscribed).
		Str("build", build.Version).
		Msg("goodnotes sub refresh applied")
}
