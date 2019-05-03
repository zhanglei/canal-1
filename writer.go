package canal

import (
	"bufio"
	"io"
)

// Writer is a specialized RESP Value type writer.
type Writer struct {
	wr  *bufio.Writer
	cur []byte
}

// NewWriter returns a new Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		wr:  bufio.NewWriter(w),
		cur: make([]byte, 0),
	}
}

// write cur
func _copy(dest *[]byte, src []byte) {
	*dest = make([]byte, len(src))
	copy(*dest, src)
}

// WriteValue writes a RESP Value.
func (wr *Writer) WriteValue(v Value) error {
	b, err := v.MarshalRESP()
	if err != nil {
		return err
	}
	n, err := wr.wr.Write(b)
	_copy(&wr.cur, b)
	if n < 1 {
		return &ErrProtocol{Msg: "write buf error."}
	}
	return err
}

// WriteSimpleString writes a RESP simple string. A simple string has no new lines.
// The carriage return and new line characters are replaced with spaces.
func (wr *Writer) WriteSimpleString(s string) error { return wr.WriteValue(SimpleStringValue(s)) }

// WriteBytes writes a RESP bulk string. A bulk string can represent any data.
func (wr *Writer) WriteBytes(b []byte) error { return wr.WriteValue(BytesValue(b)) }

// WriteString writes a RESP bulk string. A bulk string can represent any data.
func (wr *Writer) WriteString(s string) error { return wr.WriteValue(StringValue(s)) }

// WriteNull writes a RESP null bulk string.
func (wr *Writer) WriteNull() error { return wr.WriteValue(NullValue()) }

// WriteError writes a RESP error.
func (wr *Writer) WriteError(err error) error { return wr.WriteValue(ErrorValue(err)) }

// WriteInteger writes a RESP integer.
func (wr *Writer) WriteInteger(i int) error { return wr.WriteValue(IntegerValue(i)) }

// WriteArray writes a RESP array.
func (wr *Writer) WriteArray(vals []Value) error { return wr.WriteValue(ArrayValue(vals)) }

// WriteMultiBulk writes a RESP array which contains one or more bulk strings.
// For more information on RESP arrays and strings please see http://redis.io/topics/protocol.
func (wr *Writer) WriteMultiBulk(commandName string, args ...interface{}) error {
	return wr.WriteValue(MultiBulkValue(commandName, args...))
}

// Flush over write
func (wr *Writer) Flush() error { return wr.wr.Flush() }

// Get current command
func (wr *Writer) Get() []byte { return wr.cur }
