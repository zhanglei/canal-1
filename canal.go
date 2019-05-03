package canal

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type CommandDecoder interface {
	Command(cmd *Command) error
}

type Replica struct {
	curDB int

	r ByteReader

	rdbEvent    Decoder
	offsetEvent OffsetHandler
	cmdEvent    CommandDecoder
	wr          io.Writer
}

func NewReplica(r io.Reader, e Decoder, o OffsetHandler, c CommandDecoder) *Replica {
	return &Replica{
		0, bufio.NewReader(r), e, o, c, nil,
	}
}

func (r *Replica) SetWriter(w io.Writer) {
	r.wr = w
}

func (r *Replica) DumpFromFile() error {
	err := DecodeStream(r.r, r.rdbEvent)
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

		log.Printf("current opcode %s length %d\n", val.String(), n)
		if isMark {
			r.offsetEvent.Increment(int64(n))
		}

		switch val.Type() {
		case SimpleString:
			if strings.HasPrefix(val.String(), "CONTINUE") {
				isMark = true
			}
			log.Printf("recevice simple string %s\n", val.String())
		case Error:
			return errors.New(val.String())
		case Integer:
			continue
		case BulkString:
			log.Printf("recevice bulk string %s\n", val.String())
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
			val, _, err = resp.ReadValue() // read crc64 check sum
			if err != nil {
				return err
			}
			log.Printf("crc64=%d\n", Digest([]byte(val.String())))
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
	mu sync.Mutex

	offset  int64
	conn    Conn
	replica *Replica
}
