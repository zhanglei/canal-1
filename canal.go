package canal

import (
	"bytes"
	"log"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Config struct {
	Address string
}

func NewConfig(addr string) *Config {
	return &Config{Address: addr}
}

type Canal struct {
	conn net.Conn

	replica *replica

	cmder CommandDecoder

	db  int
	cfg *Config

	runID  string
	offset int64

	once   sync.Once
	closeC chan *Canal
	closeR chan *replica
}

func NewCanal(cfg *Config) (*Canal, error) {
	c := new(Canal)
	c.once = sync.Once{}
	c.closeC = make(chan *Canal)
	c.closeR = make(chan *replica)
	c.cfg = cfg
	err := c.prepare()
	if err != nil {
		return nil, err
	}
	err = c.replconf()
	if err != nil {
		return nil, err
	}
	c.replica = newReplica(c.conn, c)
	return c, nil
}

func (c *Canal) Run(commandDecode CommandDecoder) error {
	if commandDecode == nil {
		return errors.Errorf("command decode is nil.")
	}
	c.cmder = commandDecode
	return c.replica.dumpAndParse(c.closeR)
}

func (c *Canal) Close() {
	c.closeR <- c.replica
	c.closeC <- c
}

func (c *Canal) prepare() error {
	conn, err := net.Dial("tcp", c.cfg.Address)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Canal) ack() {
	go func() {
		secondDo := func() {
			ticker := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-c.closeC:
					log.Printf("[CANAL] shutdown.")
					return
				default:
				}
				ack, _ := MultiBulkBytes(MultiBulkValue("replconf", "ack", c.Offset()))
				c.conn.Write([]byte(ack))
				<-ticker.C
			}
		}
		c.once.Do(secondDo)
	}()
}

func getAddr(conn net.Conn) (ip string, port string, err error) {
	localAddr, ok := conn.LocalAddr().(*net.TCPAddr)
	if !ok {
		return "", "", errors.Errorf("connection is invalid ?")
	}
	return net.SplitHostPort(localAddr.String())
}

func (c *Canal) replconf() error {
	ip, port, err := getAddr(c.conn)
	if err != nil {
		return err
	}
	_rd := NewReader(c.conn)

	listening_port, _ := MultiBulkBytes(MultiBulkValue("REPLCONF", "listening-port", port))
	if _, err = c.conn.Write(listening_port); err != nil {
		return err
	}
	reply, _, err := _rd.readLine()
	if err != nil {
		return err
	}
	if bytes.Contains(reply, []byte("OK")) {
		log.Printf("[CANAL] replconf listening port success.\n")
	} else {
		log.Printf("[CANAL] replconf listening port failed.\n")
	}

	ip_address, _ := MultiBulkBytes(MultiBulkValue("REPLCONF", "ip-address", ip))
	if _, err = c.conn.Write(ip_address); err != nil {
		return err
	}
	reply, _, err = _rd.readLine()
	if err != nil {
		return err
	}
	if bytes.Contains(reply, []byte("OK")) {
		log.Printf("[CANAL] replconf ip address success.\n")
	} else {
		log.Printf("[CANAL] replconf ip address failed.\n")
	}

	capaEof, _ := MultiBulkBytes(MultiBulkValue("REPLCONF", "capa", "eof"))
	if _, err = c.conn.Write(capaEof); err != nil {
		return err
	}
	reply, _, err = _rd.readLine()
	if err != nil {
		return err
	}
	if bytes.Contains(reply, []byte("OK")) {
		log.Printf("[CANAL] replconf capa model success.\n")
	} else {
		log.Printf("[CANAL] replconf capa model failed.\n")
	}

	capaMethod, _ := MultiBulkBytes(MultiBulkValue("REPLCONF", "capa", "psync2"))
	if _, err = c.conn.Write(capaMethod); err != nil {
		return err
	}
	reply, _, err = _rd.readLine()
	if err != nil {
		return err
	}
	if bytes.Contains(reply, []byte("OK")) {
		log.Printf("[CANAL] replconf capa method success.\n")
	} else {
		log.Printf("[CANAL] replconf capa method failed.\n")
	}

	if c.runID == "" {
		c.runID = "?"
		c.offset = -1
	}

	psync, _ := MultiBulkBytes(MultiBulkValue("psync", c.runID, c.Offset()))
	_, err = c.conn.Write(psync)
	if err != nil {
		return err
	}
	return nil
}
