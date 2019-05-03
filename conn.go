package canal

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

func cloneTLSConfig(cfg *tls.Config) *tls.Config {
	return cfg.Clone()
}

type Conn struct {
	c     net.Conn
	ip    string
	port  int
	nodes map[string]*Conn
}

func (s *Conn) Conn() net.Conn { return s.c }

func (s *Conn) LocalIp() string { return s.ip }

func (s *Conn) LocalPort() int { return s.port }

func (s *Conn) Node() map[string]net.Conn {
	k := strings.Join(
		[]string{
			s.ip,
			fmt.Sprintf("%d", s.port)},
		":",
	)
	return map[string]net.Conn{k: s.c}
}

func Dial(network, address string, options ...DialOption) (cli *Conn, err error) {
	conn, ip, port, err := dial(network, address, options...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Conn{c: conn, ip: ip, port: port}, nil
}

// find local host usable port
// discard applying dial
func getport() (ip net.IP, port int, err error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, -1, err
	}
	tcpAddr := l.Addr().(*net.TCPAddr)
	ip = tcpAddr.IP
	port = tcpAddr.Port
	return ip, port, l.Close()
}

// dial connects to the Redis server at the given network and
// address using the specified options.
func dial(network, address string, options ...DialOption) (
	netConn net.Conn,
	ip string,
	port int,
	err error,
) {

	do := dialOptions{
		dialer: &net.Dialer{
			KeepAlive: time.Minute * 5,
		},
	}
	for _, option := range options {
		option.f(&do)
	}

	if do.dial == nil {
		do.dial = do.dialer.Dial
	}

	if netConn, err = do.dial(network, address); err != nil {
		return nil, "", -1, errors.WithStack(err)
	}

	ipAddr := strings.Split(netConn.LocalAddr().String(), ":")
	ip = ipAddr[0]
	portInt64, err := strconv.ParseInt(ipAddr[1], 10, 64)
	if err != nil {
		return nil, "", -1, errors.WithStack(err)
	}
	port = int(portInt64)

	_, err = netConn.Write([]byte("ping"))
	if err != nil {
		return nil, "", -1, errors.New("ping error.")
	}

	if do.useTLS {
		var tlsConfig *tls.Config
		if do.tlsConfig == nil {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: do.skipVerify,
			}
		} else {
			tlsConfig = cloneTLSConfig(do.tlsConfig)
		}
		if tlsConfig.ServerName == "" {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				netConn.Close()
				return nil, "", -1, errors.WithStack(err)
			}
			tlsConfig.ServerName = host
		}

		tlsConn := tls.Client(netConn, tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			netConn.Close()
			return nil, "", -1, errors.WithStack(err)
		}
		netConn = tlsConn

	}

	// if do.password != "" {
	// 	if _, err := c.Do("AUTH", do.password); err != nil {
	// 		netConn.Close()
	// 		return nil, err
	// 	}
	// }

	// if do.clientName != "" {
	// 	if _, err := c.Do("CLIENT", "SETNAME", do.clientName); err != nil {
	// 		netConn.Close()
	// 		return nil, err
	// 	}
	// }

	// if do.db != 0 {
	// 	if _, err := c.Do("SELECT", do.db); err != nil {
	// 		netConn.Close()
	// 		return nil, err
	// 	}
	// }

	return netConn, ip, port, nil
}
