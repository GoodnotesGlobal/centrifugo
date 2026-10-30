package centrifuge

// AppliedRefreshState returns, under the client lock, the connection expiration Unix time the
// node currently applies (0 means no expiration) and whether the client is closed.
//
// Goodnotes refresh-applied witness v1: a read-only accessor added to the official source at
// 84d40cc4dbf6. It changes no state and no behaviour; the receiver-side witness reads it right
// after a server-side refresh callback returns.
func (c *Client) AppliedRefreshState() (expireAt int64, closed bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.exp, c.status == statusClosed
}

// AppliedSubscriptionState returns, under the client lock, the subscription expiration Unix time
// the node currently applies for channel (0 means none) and whether the client is still subscribed.
//
// Read-only accessor for the same witness. A refused server-side sub refresh unsubscribes
// asynchronously (see updatePresence), so subscribed=true right after a refusal proves nothing.
func (c *Client) AppliedSubscriptionState(channel string) (expireAt int64, subscribed bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	channelContext, ok := c.channels[channel]
	if !ok {
		return 0, false
	}
	return channelContext.expireAt, true
}
