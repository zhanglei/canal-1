package canal

import (
	"crypto/tls"
	"net"
	"time"
)

// DialOption specifies an option for dialing a Redis server.
type DialOption struct {
	f func(*dialOptions)
}

type dialOptions struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialer       *net.Dialer
	dial         func(network, addr string) (net.Conn, error)
	db           int
	password     string
	clientName   string
	useTLS       bool
	skipVerify   bool
	tlsConfig    *tls.Config
}

// DialReadTimeout specifies the timeout for reading a single command reply.
func DialReadTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.readTimeout = d
	}}
}

// DialWriteTimeout specifies the timeout for writing a single command.
func DialWriteTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.writeTimeout = d
	}}
}

// DialConnectTimeout specifies the timeout for connecting to the Redis server when
// no DialNetDial option is specified.
func DialConnectTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.Timeout = d
	}}
}

// DialKeepAlive specifies the keep-alive period for TCP connections to the Redis server
// when no DialNetDial option is specified.
// If zero, keep-alives are not enabled. If no DialKeepAlive option is specified then
// the default of 5 minutes is used to ensure that half-closed TCP sessions are detected.
func DialKeepAlive(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.KeepAlive = d
	}}
}

// DialNetDial specifies a custom dial function for creating TCP
// connections, otherwise a net.Dialer customized via the other options is used.
// DialNetDial overrides DialConnectTimeout and DialKeepAlive.
func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dial = dial
	}}
}

// DialDatabase specifies the database to select when dialing a connection.
func DialDatabase(db int) DialOption {
	return DialOption{func(do *dialOptions) {
		do.db = db
	}}
}

// DialPassword specifies the password to use when connecting to
// the Redis server.
func DialPassword(password string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.password = password
	}}
}

// DialClientName specifies a Conn name to be used
// by the Redis server connection.
func DialClientName(name string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.clientName = name
	}}
}

// DialTLSConfig specifies the config to use when a TLS connection is dialed.
// Has no effect when not dialing a TLS connection.
func DialTLSConfig(c *tls.Config) DialOption {
	return DialOption{func(do *dialOptions) {
		do.tlsConfig = c
	}}
}

// DialTLSSkipVerify disables server name verification when connecting over
// TLS. Has no effect when not dialing a TLS connection.
func DialTLSSkipVerify(skip bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.skipVerify = skip
	}}
}

// DialUseTLS specifies whether TLS should be used when connecting to the
// server. This option is ignore by DialURL.
func DialUseTLS(useTLS bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.useTLS = useTLS
	}}
}
