package proxy

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/centrifugal/centrifugo/v6/internal/configtypes"
	"github.com/centrifugal/centrifugo/v6/internal/tools"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// Goodnotes refresh-applied witness v1: every refresh proxy branch is typed, and the reply/error
// values stay exactly the upstream ones.
func TestGoodnotesRefreshOutcomeTypesEveryHTTPBranch(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		outcome  RefreshOutcome
		expired  bool
		expireAt int64 // 0 = assert none, -1 = assert now+60 grace
		wantErr  bool
	}{
		{"backend expire_at", 200, `{"result":{"expire_at":1824536000}}`, RefreshOutcomeBackendExpireAt, false, 1824536000, false},
		{"backend expired", 200, `{"result":{"expired":true}}`, RefreshOutcomeBackendExpired, true, 0, false},
		{"empty response", 200, `{}`, RefreshOutcomeNoResult, true, 0, false},
		// Conditional upstream mapping: this handler reads only result, so a disconnect-only
		// body takes the no-result branch. Observed here for the exact source, not asserted live.
		{"disconnect-only body", 200, `{"disconnect":{"code":4201,"reason":"session invalidated"}}`, RefreshOutcomeNoResult, true, 0, false},
		{"proxy http error", 500, ``, RefreshOutcomeProxyErrorGrace60, false, -1, false},
		{"bad b64info", 200, `{"result":{"expire_at":1824536000,"b64info":"%%%"}}`, RefreshOutcomeDecodeError, false, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpCase := newRefreshHandlerHTTPTestCase(context.Background(), "/refresh")
			defer httpCase.Teardown()
			httpCase.Mux.HandleFunc("/refresh", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			before := time.Now().Unix()
			reply, extra, err := httpCase.refreshProxyHandler.Handle()(httpCase.Client, centrifuge.RefreshEvent{}, PerCallData{})
			require.Equal(t, tc.outcome, extra.Outcome)
			require.Equal(t, tc.expired, reply.Expired)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			switch {
			case tc.expireAt == -1:
				require.GreaterOrEqual(t, reply.ExpireAt, before+60)
				require.LessOrEqual(t, reply.ExpireAt, time.Now().Unix()+60)
			default:
				require.Equal(t, tc.expireAt, reply.ExpireAt)
			}
		})
	}
}

func TestGoodnotesSubRefreshOutcomeTypesEveryHTTPBranch(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		outcome  RefreshOutcome
		expired  bool
		expireAt int64 // -1 = assert now+60 grace
		wantErr  bool
	}{
		{"backend expire_at", 200, `{"result":{"expire_at":1824536000}}`, RefreshOutcomeBackendExpireAt, false, 1824536000, false},
		{"backend expired", 200, `{"result":{"expired":true}}`, RefreshOutcomeBackendExpired, true, 0, false},
		{"empty response", 200, `{}`, RefreshOutcomeNoResult, true, 0, false},
		{"proxy http error", 500, ``, RefreshOutcomeProxyErrorGrace60, false, -1, false},
		{"bad b64info", 200, `{"result":{"expire_at":1824536000,"b64info":"%%%"}}`, RefreshOutcomeDecodeError, false, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			common := tools.NewCommonHTTPProxyTestCase(context.Background())
			defer common.Teardown()
			common.Mux.HandleFunc("/sub_refresh", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			p, err := NewHTTPSubRefreshProxy(getTestHttpProxy(common, "/sub_refresh"))
			require.NoError(t, err)
			handler := NewSubRefreshHandler(SubRefreshHandlerConfig{Proxies: map[string]SubRefreshProxy{"default": p}}).Handle()
			opts := configtypes.ChannelOptions{SubRefreshProxyEnabled: true, SubRefreshProxyName: "default"}
			before := time.Now().Unix()
			reply, extra, err := handler(common.Client, centrifuge.SubRefreshEvent{Channel: "test"}, opts, PerCallData{})
			require.Equal(t, tc.outcome, extra.Outcome)
			require.Equal(t, tc.expired, reply.Expired)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.expireAt == -1 {
				require.GreaterOrEqual(t, reply.ExpireAt, before+60)
				require.LessOrEqual(t, reply.ExpireAt, time.Now().Unix()+60)
			} else {
				require.Equal(t, tc.expireAt, reply.ExpireAt)
			}
		})
	}
	disabled, _, err := NewSubRefreshHandler(SubRefreshHandlerConfig{Proxies: map[string]SubRefreshProxy{}}).Handle()(
		nil, centrifuge.SubRefreshEvent{Channel: "test"}, configtypes.ChannelOptions{}, PerCallData{})
	require.Error(t, err)
	require.False(t, disabled.Expired)
}
