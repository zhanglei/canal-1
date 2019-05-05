package canal

import (
	"bufio"
	"io"
	"strings"

	"github.com/pkg/errors"
)

type acker interface {
	ack()
}

type canaler interface {
	Decoder
	OffsetHandler
	CommandDecoder
	acker
}

type replica struct {
	r ByteReader
	c canaler
}

func newReplica(rd io.Reader, c canaler) *replica {
	return &replica{bufio.NewReader(rd), c}
}

func (r *replica) dumpFromFile() error {
	err := DecodeFile(r.r, r.c)
	if err != nil {
		return err
	}
	return nil
}

func (r *replica) dumpAndParse(close chan struct{}) error {
	isMark := false
	resp := NewReader(r.r)
	for {
		select {
		case <-close:
			return nil
		default:
		}
		val, n, err := resp.ReadValue()
		if err != nil {
			return err
		}
		if isMark {
			r.c.Increment(int64(n))
		}
		switch val.Type() {
		case SimpleString:
			if strings.HasPrefix(val.String(), "CONTINUE") {
				isMark = true
			}
		case Error:
			continue
		case Integer:
			continue
		case BulkString:
			if strings.HasPrefix(val.String(), "PING") { // first ping lost 4 bit `*1\r\n`
				r.c.Increment(int64(4))
			}

		case Array:
			cmd, err := NewCommand(buildStrCommand(val.String())...)
			if err != nil {
				return err
			}
			r.c.Command(cmd)
		case Rdb:
			_, offset := val.ReplInfo()
			r.c.Increment(offset)

			err = DecodeStream(r.r, r.c)
			if err != nil {
				return err
			}
			// skip crc64 check sum
			_, _, err = resp.ReadValue()
			if err != nil {
				return err
			}
			isMark = true
		case CRLF:
			continue
		default:
			return errors.Errorf("unknow opcode %v.", val)
		}
		r.c.ack()
	}
}
