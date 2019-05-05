// +build ignore

package canal

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

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
	once := sync.Once{}
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
			if strings.HasPrefix(val.String(), "PING") { // first ping lost 4 bit `*1\r\n`
				r.offsetEvent.Increment(int64(4))
			}

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

		go func() {
			secondDo := func() {
				ticker := time.NewTicker(1 * time.Second)
				for {
					ack := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" +
						fmt.Sprintf("%d", len(r.offsetEvent.Offset())) +
						"\r\n" +
						r.offsetEvent.Offset() +
						"\r\n"
					r.wr.Write([]byte(ack))
					<-ticker.C
				}
			}
			once.Do(secondDo)
		}()

		_, _, _ = val, n, err
	}
}
