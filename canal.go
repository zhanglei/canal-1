package canal

import (
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
	closeC chan struct{}
	closeR chan struct{}
}

func NewCanal(cfg *Config) (*Canal, error) {
	c := new(Canal)
	c.once = sync.Once{}
	c.closeC = make(chan struct{})
	c.closeR = make(chan struct{})
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
	close := struct{}{}
	c.closeR <- close
	c.closeC <- close
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

func (c *Canal) replconf() error {
	// _, err := c.conn.Do("REPLCONF", "listening-port", "0")
	// if err != nil {
	// 	return err
	// }

	// if _, err := c.conn.Do("REPLCONF", "ip-address", "172.17.0.1"); err != nil {
	// 	return err
	// }

	// if _, err := c.conn.Do("REPLCONF", "capa", "eof"); err != nil {
	// 	return err
	// }

	// if _, err := c.conn.Do("REPLCONF", "capa", "psync2"); err != nil {
	// 	return err
	// }

	if c.runID == "" {
		c.runID = "?"
		c.offset = -1
	}

	// ack := "*3\r\n$5\r\npsync\r\n$" +
	// 	fmt.Sprintf("%d", len(c.runID)) +
	// 	"\r\n" +
	// 	c.runID +
	// 	"\r\n" +
	// 	"$" + fmt.Sprintf("%d", len(c.Offset())) +
	// 	"\r\n" +
	// 	c.Offset() +
	// 	"\r\n"
	psync, _ := MultiBulkBytes(MultiBulkValue("psync", c.runID, c.Offset()))
	_, err := c.conn.Write(psync)
	if err != nil {
		return err
	}
	return nil
}
