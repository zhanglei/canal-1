package canal

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
)

type Replica struct {
	r ByteReader

	rdbEvent    Decoder
	offsetEvent OffsetHandler
	cmdEvent    CommandDecoder

	wr io.Writer
}

func NewReplica(r io.Reader, e Decoder, o OffsetHandler, c CommandDecoder) *Replica {
	return &Replica{
		bufio.NewReader(r), e, o, c, nil,
	}
}

func (r *Replica) SetWriter(w io.Writer) {
	r.wr = w
}

func (r *Replica) DumpFromFile() error {
	err := DecodeFile(r.r, r.rdbEvent)
	if err != nil {
		return err
	}
	return nil
}

func (r *Replica) DumpAndParse() error {
	isMark := false
	resp := NewReader(r.r)
	for {
		val, n, err := resp.ReadValue()
		if err != nil {
			return err
		}
		if isMark {
			r.offsetEvent.Increment(int64(n))
		}
		switch val.Type() {
		case SimpleString:
			if strings.HasPrefix(val.String(), "CONTINUE") {
				isMark = true
			}
		case Error:
			return errors.New(val.String())
		case Integer:
			continue
		case BulkString:
			continue
		case Array:
			cmd, err := NewCommand(buildStrCommand(val.String())...)
			if err != nil {
				return err
			}
			r.cmdEvent.Command(cmd)
		case Rdb:
			_, offset := val.ReplInfo()
			r.offsetEvent.Increment(offset)

			err = DecodeStream(r.r, r.rdbEvent)
			if err != nil {
				return err
			}
			// skip crc64 check sum
			val, _, err = resp.ReadValue()
			if err != nil {
				return err
			}
			isMark = true
		case CRLF:
			continue
		default:
			return errors.New("unknow opcode.")
		}
		ack := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + fmt.Sprintf("%d", len(r.offsetEvent.Offset())) + "\r\n" + r.offsetEvent.Offset() + "\r\n"
		r.wr.Write([]byte(ack))
		_, _, _ = val, n, err
	}
}

type Canal struct {
	conn Conn

	replica *Replica

	cmdEvent CommandDecoder

	db     int
	offset int64
}

func (c *Canal) set(n int64) {
	atomic.StoreInt64(&c.offset, n)
}

func (c *Canal) Increment(n int64) {
	atomic.AddInt64(&c.offset, n)
}

func (c *Canal) Offset() string {
	return fmt.Sprintf("%d", atomic.LoadInt64(&c.offset))
}

func (c *Canal) BeginRDB() {}

func (c *Canal) BeginDatabase(n int) {
	c.db = n
	cmd, _ := NewCommand("SELECT", fmt.Sprintf("%d", n))
	c.cmdEvent.Command(cmd)
}

func (c *Canal) Aux(key, value []byte) {
	if string(key) == "repl-offset" {
		i, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			panic(err)
		}
		c.set(i)
	}
}

func (c *Canal) ResizeDatabase(dbSize, expiresSize uint32) {}

func (c *Canal) EndDatabase(n int) {}

func (c *Canal) Set(key, value []byte, expiry int64) {
	cmd, _ := NewCommand("SET", string(key), string(value))
	c.cmdEvent.Command(cmd)
}

func (c *Canal) BeginHash(key []byte, length, expiry int64) {}

func (c *Canal) Hset(key, field, value []byte) {
	cmd, _ := NewCommand("HSET", string(key), string(field), string(value))
	c.cmdEvent.Command(cmd)
}
func (c *Canal) EndHash(key []byte) {}

func (c *Canal) BeginSet(key []byte, cardinality, expiry int64) {}

func (c *Canal) Sadd(key, member []byte) {
	cmd, _ := NewCommand("SADD", string(key), string(member))
	c.cmdEvent.Command(cmd)
}
func (c *Canal) EndSet(key []byte) {}

func (c *Canal) BeginList(key []byte, length, expiry int64) {}

func (c *Canal) Rpush(key, value []byte) {
	cmd, _ := NewCommand("RPUSH", string(key), string(value))
	c.cmdEvent.Command(cmd)
}
func (c *Canal) EndList(key []byte) {}

func (c *Canal) BeginZSet(key []byte, cardinality, expiry int64) {}

func (c *Canal) Zadd(key []byte, score float64, member []byte) {
	cmd, _ := NewCommand("ZADD", string(key), fmt.Sprintf("%f", score), string(member))
	c.cmdEvent.Command(cmd)
}
func (c *Canal) EndZSet(key []byte) {}

func (c *Canal) BeginStream(key []byte, cardinality, expiry int64) {}
func (c *Canal) Xadd(key, id, listpack []byte) {
	cmd, _ := NewCommand("XADD", string(key), string(id), string(listpack))
	c.cmdEvent.Command(cmd)
}
func (c *Canal) EndStream(key []byte) {}

func (c *Canal) EndRDB() {}
