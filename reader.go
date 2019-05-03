package canal

import (
	"bufio"
	"io"
	"strconv"
)

// Reader is a specialized RESP Value type reader.
type Reader struct {
	rd *bufio.Reader
}

// NewReader returns a Reader for reading Value types.
func NewReader(rd io.Reader) *Reader {
	r := &Reader{rd: bufio.NewReader(rd)}
	return r
}

// ReadValue reads the next Value from Reader.
func (rd *Reader) ReadValue() (value Value, n int, err error) {
	value, _, n, err = rd.readValue(false, false)
	return
}

// ReadMultiBulk reads the next multi bulk Value from Reader.
// A multi bulk value is a RESP ArrayV that contains one or more bulk strings.
// For more information on RESP arrays and strings please see http://redis.io/topics/protocol.
func (rd *Reader) ReadMultiBulk() (value Value, telnet bool, n int, err error) {
	return rd.readValue(true, false)
}

func (rd *Reader) readValue(multibulk, child bool) (val Value, telnet bool, n int, err error) {
	var rn int
	var c byte
	c, err = rd.rd.ReadByte()
	if err != nil {
		return NilValue, false, n, err
	}
	n++
	if c == '*' {
		val, rn, err = rd.readArrayValue(multibulk)
	} else if multibulk && !child {
		telnet = true
	} else {
		switch c {
		default:
			if multibulk && child {
				return NilValue, telnet, n, &ErrProtocol{Msg: "expected '$', got '" + string(c) + "'"}
			}
			if child {
				return NilValue, telnet, n, &ErrProtocol{Msg: "unknown first byte"}
			}
			telnet = true
		case '-', '+':
			val, rn, err = rd.readSimpleValue(c)
		case ':':
			val, rn, err = rd.readIntegerValue()
		case '$':
			val, rn, err = rd.readBulkValue()
		case '\r':
			val, rn, err = rd.readCRLF()
		}
	}
	if telnet {
		n--
		rd.rd.UnreadByte()
		val, rn, err = rd.readTelnetMultiBulk()
		if err == nil {
			telnet = true
		}
	}
	n += rn
	if err == io.EOF {
		return NilValue, telnet, n, io.ErrUnexpectedEOF
	}
	return val, telnet, n, err
}

func (rd *Reader) readTelnetMultiBulk() (v Value, n int, err error) {
	values := make([]Value, 0, 8)
	var c byte
	var bline []byte
	var quote, mustspace bool
	for {
		c, err = rd.rd.ReadByte()
		if err != nil {
			return NilValue, n, err
		}
		n += 1
		if c == '\n' {
			if len(bline) > 0 && bline[len(bline)-1] == '\r' {
				bline = bline[:len(bline)-1]
			}
			break
		}
		if mustspace && c != ' ' {
			return NilValue, n, &ErrProtocol{Msg: "unbalanced quotes in request"}
		}
		if c == ' ' {
			if quote {
				bline = append(bline, c)
			} else {
				values = append(values, Value{Typ: '$', Str: bline})
				bline = nil
			}
		} else if c == '"' {
			if quote {
				mustspace = true
			} else {
				if len(bline) > 0 {
					return NilValue, n, &ErrProtocol{Msg: "unbalanced quotes in request"}
				}
				quote = true
			}
		} else {
			bline = append(bline, c)
		}
	}
	if quote {
		return NilValue, n, &ErrProtocol{Msg: "unbalanced quotes in request"}
	}
	if len(bline) > 0 {
		values = append(values, Value{Typ: '$', Str: bline})
	}
	return Value{Typ: '*', ArrayV: values}, n, nil
}

func (rd *Reader) readSimpleValue(Typ byte) (val Value, n int, err error) {
	var line []byte
	line, n, err = rd.readLine()
	if err != nil {
		return NilValue, n, err
	}
	return Value{Typ: Type(Typ), Str: line}, n, nil
}

func (rd *Reader) readLine() (line []byte, n int, err error) {
	for {
		b, err := rd.rd.ReadBytes('\n')
		if err != nil {
			return nil, 0, err
		}
		n += len(b)
		line = append(line, b...)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (rd *Reader) readBulkValue() (val Value, n int, err error) {
	var rn int
	var l int
	l, rn, err = rd.readInt()
	n += rn
	if err != nil {
		if _, ok := err.(*ErrProtocol); ok {
			return NilValue, n, &ErrProtocol{Msg: "invalid bulk length"}
		}
		return NilValue, n, err
	}
	if l < 0 {
		return Value{Typ: '$', Null: true}, n, nil
	}
	if l > 512*1024*1024 {
		return NilValue, n, &ErrProtocol{Msg: "invalid bulk length"}
	}
	b := make([]byte, l+2)
	rn, err = io.ReadFull(rd.rd, b)
	n += rn
	if err != nil {
		return NilValue, n, err
	}
	if b[l] != '\r' || b[l+1] != '\n' {
		return NilValue, n, &ErrProtocol{Msg: "invalid bulk line ending"}
	}
	return Value{Typ: '$', Str: b[:l]}, n, nil
}

func (rd *Reader) readArrayValue(multibulk bool) (val Value, n int, err error) {
	var rn int
	var l int
	l, rn, err = rd.readInt()
	n += rn
	if err != nil || l > 1024*1024 {
		if _, ok := err.(*ErrProtocol); ok {
			if multibulk {
				return NilValue, n, &ErrProtocol{Msg: "invalid multibulk length"}
			}
			return NilValue, n, &ErrProtocol{Msg: "invalid ArrayV length"}
		}
		return NilValue, n, err
	}
	if l < 0 {
		return Value{Typ: '*', Null: true}, n, nil
	}
	var aval Value
	vals := make([]Value, l)
	for i := 0; i < l; i++ {
		aval, _, rn, err = rd.readValue(multibulk, true)
		n += rn
		if err != nil {
			return NilValue, n, err
		}
		vals[i] = aval
	}
	return Value{Typ: '*', ArrayV: vals}, n, nil
}

func (rd *Reader) readIntegerValue() (val Value, n int, err error) {
	var l int
	l, n, err = rd.readInt()
	if err != nil {
		if _, ok := err.(*ErrProtocol); ok {
			return NilValue, n, &ErrProtocol{Msg: "invalid integer"}
		}
		return NilValue, n, err
	}
	return Value{Typ: ':', IntegerV: l}, n, nil
}

func (rd *Reader) readInt() (x int, n int, err error) {
	line, n, err := rd.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (rd *Reader) safeRead(n int) ([]byte, int, error) {
	buf := make([]byte, n)
	n, err := io.ReadFull(rd.rd, buf)
	return buf, n, err
}

func (rd *Reader) readCRLF() (val Value, n int, err error) {
	bs, n, err := rd.safeRead(1)
	if err != nil {
		return
	}
	val = Value{Typ: CRLF, Str: bs}
	return
}
