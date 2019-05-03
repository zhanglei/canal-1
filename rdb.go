package canal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/pkg/errors"
)

func DecodeStream(r io.Reader, d Decoder) error {
	decoder := &rdbDecode{d, make([]byte, 8), bufio.NewReader(r)}
	return decoder.decode()
}

func DecodeDump(dump []byte, db int, key []byte, expiry int64, d Decoder) error {
	err := verifyDump(dump)
	if err != nil {
		return err
	}
	decoder := &rdbDecode{d, make([]byte, 8), bytes.NewReader(dump[1:])}
	decoder.event.BeginRDB()
	decoder.event.BeginDatabase(db)
	err = decoder.readObject(key, ValueType(dump[0]), expiry)
	decoder.event.EndDatabase(db)
	decoder.event.EndRDB()
	return err
}

type rdbDecode struct {
	event  Decoder
	intBuf []byte
	r      ByteReader
}

func (d *rdbDecode) Parse(dr Decoder) error {
	d.event = dr
	return d.decode()
}

func (d *rdbDecode) decode() error {
	err := d.checkHeader()
	if err != nil {
		return err
	}
	d.event.BeginRDB()
	var db uint64
	var expiry int64
	//var lruClock int64
	var lruIdle uint64
	var lfuFreq int
	firstDB := true
	for {
		objType, err := d.r.ReadByte()
		if err != nil {
			return errors.Wrap(err, "readfailed")
		}
		switch objType {
		case rdbOpCodeFreq:
			b, err := d.r.ReadByte()
			lfuFreq = int(b)
			if err != nil {
				return err
			}
		case rdbOpCodeIdle:
			idle, _, err := d.readLength()
			if err != nil {
				return err
			}
			lruIdle = uint64(idle)
		case rdbOpCodeAux:
			auxKey, err := d.readString()
			if err != nil {
				return err
			}
			auxVal, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Aux(auxKey, auxVal)
		case rdbOpCodeResizeDB:
			dbSize, _, err := d.readLength()
			if err != nil {
				return err
			}
			expiresSize, _, err := d.readLength()
			if err != nil {
				return err
			}
			d.event.ResizeDatabase(uint32(dbSize), uint32(expiresSize))
		case rdbOpCodeExpiryMS:
			_, err := io.ReadFull(d.r, d.intBuf)
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint64(d.intBuf))
		case rdbOpCodeExpiry:
			_, err := io.ReadFull(d.r, d.intBuf[:4])
			if err != nil {
				return err
			}
			expiry = int64(binary.LittleEndian.Uint32(d.intBuf)) * 1000
		case rdbOpCodeSelectDB:
			if !firstDB {
				d.event.EndDatabase(int(db))
			}
			db, _, err = d.readLength()
			if err != nil {
				return err
			}
			d.event.BeginDatabase(int(db))
		case rdbOpCodeEOF:
			d.event.EndDatabase(int(db))
			d.event.EndRDB()
			return nil
		case rdbOpCodeModuleAux:

		default:
			key, err := d.readString()
			if err != nil {
				return err
			}
			err = d.readObject(key, ValueType(objType), expiry)
			if err != nil {
				return err
			}
			_, _ = lfuFreq, lruIdle
			expiry = 0
			lfuFreq = 0
			lruIdle = 0
		}
	}
}

func (d *rdbDecode) readObject(key []byte, typ ValueType, expiry int64) error {
	switch typ {
	case TypeString:
		value, err := d.readString()
		if err != nil {
			return err
		}
		d.event.Set(key, value, expiry)
	case TypeList:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginList(key, int64(length), expiry)
		for length > 0 {
			length--
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Rpush(key, value)
		}
		d.event.EndList(key)
	case TypeListQuicklist:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginList(key, int64(-1), expiry)
		for length > 0 {
			length--
			d.readZiplist(key, 0, false)
		}
		d.event.EndList(key)
	case TypeSet:
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginSet(key, int64(cardinality), expiry)
		for cardinality > 0 {
			cardinality--
			member, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Sadd(key, member)
		}
		d.event.EndSet(key)
	case TypeZSet2:
		fallthrough
	case TypeZSet:
		cardinality, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginZSet(key, int64(cardinality), expiry)
		for cardinality > 0 {
			cardinality--
			member, err := d.readString()
			if err != nil {
				return err
			}
			var score float64
			if typ == TypeZSet2 {
				score, err = d.readBinaryFloat64()
				if err != nil {
					return err
				}
			} else {
				score, err = d.readFloat64()
				if err != nil {
					return err
				}
			}
			d.event.Zadd(key, score, member)
		}
		d.event.EndZSet(key)
	case TypeHash:
		length, _, err := d.readLength()
		if err != nil {
			return err
		}
		d.event.BeginHash(key, int64(length), expiry)
		for length > 0 {
			length--
			field, err := d.readString()
			if err != nil {
				return err
			}
			value, err := d.readString()
			if err != nil {
				return err
			}
			d.event.Hset(key, field, value)
		}
		d.event.EndHash(key)
	case TypeHashZipmap:
		return d.readZipmap(key, expiry)
	case TypeListZiplist:
		return d.readZiplist(key, expiry, true)
	case TypeSetIntset:
		return d.readIntset(key, expiry)
	case TypeZSetZiplist:
		return d.readZiplistZset(key, expiry)
	case TypeHashZiplist:
		return d.readZiplistHash(key, expiry)
	case TypeStreamListPacks:
		return d.readStream(key, expiry)
	case TypeModule:
		fallthrough
	case TypeModule2:
		return d.readModule(key, expiry)
	default:
		return fmt.Errorf("rdb: unknown object type %d for key %s", typ, key)
	}
	return nil
}

func (d *rdbDecode) readModule(key []byte, expiry int64) error {
	moduleid, _, err := d.readLength()
	if err != nil {
		return err
	}
	return fmt.Errorf("Not supported load module %v", moduleid)
}

func (d *rdbDecode) readStreamID() ([]byte, error) {
	entrys, err := d.readString()
	if err != nil {
		return nil, err
	}
	slb := newSliceBuffer(entrys)
	ms, err := slb.Slice(8)
	if err != nil {
		return nil, err
	}
	seq, _ := slb.Slice(8)
	if err != nil {
		return nil, err
	}
	ID := []byte(fmt.Sprintf("%d-%d",
		binary.BigEndian.Uint64(ms),
		binary.BigEndian.Uint64(seq),
	))

	return ID, nil
}

func (d *rdbDecode) readStream(key []byte, expiry int64) error {
	cardinality, _, err := d.readLength()
	if err != nil {
		return err
	}
	d.event.BeginStream(key, int64(cardinality), expiry)

	data := make([]byte, 0)

	for cardinality > 0 {
		cardinality--

		ID, err := d.readStreamID()
		if err != nil {
			return err
		}

		lpData, err := d.readString()
		if err != nil {
			return err
		}
		listpack := newSliceBuffer(lpData)

		// skip 4 byte
		// total-bytes 4
		_readListPack(listpack, 4) // total bytes
		/*
		 * Master entry
		 * +-------+---------+------------+---------+--/--+---------+---------+-+
		 * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
		 * +-------+---------+------------+---------+--/--+---------+---------+-+
		 */
		// num-elements 2
		_readListPack(listpack, 2)

		// count
		b, err := _readListPack(listpack, 2)
		if err != nil {
			return err
		}
		count := bytes2i64(b)

		// deleted
		b, err = _readListPack(listpack, 2)
		if err != nil {
			return err
		}
		deleted := bytes2i64(b)

		// num_field
		b, err = _readListPack(listpack, 2)
		if err != nil {
			return err
		}
		num_fields := bytes2i64(b)

		tempFileds := make([][]byte, num_fields)
		for i := uint64(0); i < num_fields; i++ {
			b, err := _readListPack(listpack, 1)
			if err != nil {
				return err
			}
			tempFileds[i] = b
		}
		_readListPack(listpack, 2)

		_, _, _ = count, deleted, num_fields
		total := count + deleted
		for total > 0 {
			total--
			flag, err := _readListPack(listpack, 2)
			if err != nil {
				return err
			}
			ms, _ := _readListPack(listpack, 2)
			if err != nil {
				return err
			}
			seq, _ := _readListPack(listpack, 2)
			if err != nil {
				return err
			}
			steamID := fmt.Sprintf("%d-%d", bytes2i64(ms), bytes2i64(seq))

			flagInt, _ := int(bytes2i64(flag)), steamID

			delete := false
			if (flagInt & rdbStreamItemFlagNone) != 0 {
				delete = false
			}
			_ = delete
			if (flagInt & rdbStreamItemFlangSameFields) != 0 {
				for i := 0; i < int(num_fields); i++ {
					/*
					* SAMEFIELD
					* +-------+-/-+-------+--------+
					* |value-1|...|value-N|lp-count|
					* +-------+-/-+-------+--------+
					 */
					data = append(data, tempFileds[i]...)
					data = append(data, ' ')
					value, err := _readListPack(listpack, 2)
					if err != nil {
						return err
					}
					data = append(data, value...)
					data = append(data, ' ')
				}
			} else {
				/*
				 * NONEFIELD
				 * +----------+-------+-------+-/-+-------+-------+--------+
				 * |num-fields|field-1|value-1|...|field-N|value-N|lp-count|
				 * +----------+-------+-------+-/-+-------+-------+--------+
				 */
				_numfields, err := _readListPack(listpack, 2)
				if err != nil {
					return err
				}
				for i := uint64(0); i < bytes2i64(_numfields); i++ {
					field, err := _readListPack(listpack, 1)
					if err != nil {
						return err
					}
					data = append(data, field...)
					data = append(data, ' ')

					value, err := _readListPack(listpack, 2)
					if err != nil {
						return err
					}
					data = append(data, value...)
					data = append(data, ' ')

				}
			}
			d.event.Xadd(key, ID, data)
			_readListPack(listpack, 2) // lp-count
		}
		eb, err := listpack.ReadByte() // lp-end
		if err != nil {
			return err
		}
		if eb != rdbLpEOF {
			return errors.Errorf("rdb Lp eof unexpected.")
		}

	}

	for i := 0; i < 3; i++ {
		_, _, err = d.readLength() // items , last_id, last_seq
		if err != nil {
			return err
		}
	}

	//TODO output consumer groups
	var groupsCount uint64
	groupsCount, _, err = d.readLength()
	if err != nil {
		return err
	}
	for groupsCount > 0 {
		groupsCount--
		cgname, err := d.readString()
		if err != nil {
			return err
		}
		gIDms, _, _ := d.readLength()
		gIDseq, _, _ := d.readLength()
		fmt.Printf("cgname=%s last_cg_entry_id %d-%d\n", cgname, gIDms, gIDseq)

		pelSize, _, _ := d.readLength()
		for pelSize > 0 {
			pelSize--
			eid := make([]byte, 16) //deliveryTime 8 byte deliveryCount 8 byte
			io.ReadFull(d.r, eid)
		}

		consumersNum, _, _ := d.readLength()
		for consumersNum > 0 {
			consumersNum--
			d.readString()                  // cname
			pelSize, _, _ := d.readLength() // pending
			for pelSize > 0 {
				pelSize--
				rawid := make([]byte, 16) //eid
				io.ReadFull(d.r, rawid)
			}
		}
	}

	d.event.EndStream(key)

	return nil
}

func (d *rdbDecode) readZipmap(key []byte, expiry int64) error {
	var length int
	zipmap, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(zipmap)
	lenByte, err := buf.ReadByte()
	if err != nil {
		return err
	}
	if lenByte >= 254 { // we need to count the items manually
		length, err = countZipmapItems(buf)
		length /= 2
		if err != nil {
			return err
		}
	} else {
		length = int(lenByte)
	}
	d.event.BeginHash(key, int64(length), expiry)
	for i := 0; i < length; i++ {
		field, err := readZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := readZipmapItem(buf, true)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZipmapItem(buf *sliceBuffer, readFree bool) ([]byte, error) {
	length, free, err := readZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *sliceBuffer) (int, error) {
	n := 0
	for {
		strLen, free, err := readZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func readZipmapItemLength(buf *sliceBuffer, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("rdb: invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}
	return int(b), int(free), err
}

func _readListPack(buf *sliceBuffer, length int) ([]byte, error) {
	b, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	bs, err := lpGet(b, buf)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func lpGet(b []byte, buf *sliceBuffer) ([]byte, error) {
	var vals []byte

	var uval, negstart, negmax uint64
	if lpEncodingIs7BitUint(b[0]) {
		negstart = math.MaxUint64
		negmax = 0
		uval = uint64(b[0] & 0x7F)

	} else if lpEncodingIs6BitStr(b[0]) {
		len := lpEncoding6BitStrLen(b)
		str, err := buf.Slice(int(len))
		if err != nil {
			return nil, err
		}
		return str, nil

	} else if lpEncodingIs13BitInt(b[0]) {
		tmp, err := buf.Slice(1)
		if err != nil {
			return nil, err
		}
		b = append(b, tmp...)
		uval = (uint64(b[0]&0x1f) << 8) | uint64(b[1])
		negstart = uint64(1) << 12
		negmax = 8191

	} else if lpEncodingIs16BitInt(b[0]) {
		tmp, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		b = append(b, tmp...)
		uval = uint64(b[1]) | uint64(b[2])<<8
		negstart = uint64(1) << 15
		negmax = math.MaxUint16

	} else if lpEncodingIs24BitInt(b[0]) {
		tmp, err := buf.Slice(3)
		if err != nil {
			return nil, err
		}
		b = append(b, tmp...)
		uval = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		negstart = uint64(1) << 23
		negmax = math.MaxUint32 >> 8

	} else if lpEncodingIs32BitInt(b[0]) {
		tmp, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		b = append(b, tmp...)
		uval = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 | uint64(b[4])<<24
		negstart = uint64(1) << 31
		negmax = math.MaxUint32

	} else if lpEncodingIs64BitInt(b[0]) {
		tmp, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		b = append(b, tmp...)
		uval = uint64(b[1]) |
			uint64(b[2])<<8 |
			uint64(b[3])<<16 |
			uint64(b[4])<<24 |
			uint64(b[5])<<32 |
			uint64(b[6])<<40 |
			uint64(b[7])<<48 |
			uint64(b[8])<<56
		negstart = uint64(1) << 63
		negmax = math.MaxUint64

	} else if lpEncodingIs12BitStr(b[0]) {
		tmp, err := buf.Slice(1)
		if err != nil {
			return nil, err
		}
		b = append(b, tmp...)
		len := lpEncoding12BitStrLen(b)
		str, err := buf.Slice(int(len))
		if err != nil {
			return nil, err
		}
		vals = str
		return str, nil

	} else if lpEncodingIs32BitStr(b[0]) {
		tmp, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		b = append(b, tmp...)
		len := lpEncoding32BitStrLen(b)
		str, err := buf.Slice(int(len))
		if err != nil {
			return nil, err
		}
		vals = str
		return str, nil

	} else {
		uval = uint64(12345678900000000) + uint64(b[0])
		negstart = math.MaxUint64
		negmax = 0
	}

	if uval >= negstart {
		uval = negmax - uval
		vals = i642bytes(uval)
		vals = i642bytes(bytes2i64(vals) - 1)
	} else {
		vals = i642bytes(uval)
	}
	return vals, nil
}

func i642bytes(i uint64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}
func i322bytes(i uint32) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

func i162bytes(i uint16) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint16(buf, uint16(i))
	return buf
}

func bytes2i64(buf []byte) uint64 {
	return uint64(binary.BigEndian.Uint64(buf))
}

func lpEncodingIs7BitUint(b byte) bool {
	return (((b) & rdbLpEncoding7BitUintMask) == rdbLpEncoding7BitUint)
}
func lpEncodingIs6BitStr(b byte) bool {
	return (((b) & rdbLpEncoding6BitStrMask) == rdbLpEncoding6BitStr)
}
func lpEncodingIs13BitInt(b byte) bool {
	return (((b) & rdbLpEncoding13BitIntMask) == rdbLpEncoding13BitInt)
}
func lpEncodingIs12BitStr(b byte) bool {
	return (((b) & rdbLpEncoding12BitStrMask) == rdbLpEncoding12BitStr)
}
func lpEncodingIs16BitInt(b byte) bool {
	return (((b) & rdbLpEncoding16BitIntMask) == rdbLpEncoding16BitInt)
}
func lpEncodingIs24BitInt(b byte) bool {
	return (((b) & rdbLpEncoding24BitIntMask) == rdbLpEncoding24BitInt)
}
func lpEncodingIs32BitInt(b byte) bool {
	return (((b) & rdbLpEncoding32BitIntMask) == rdbLpEncoding32BitInt)
}
func lpEncodingIs64BitInt(b byte) bool {
	return (((b) & rdbLpEncoding64BitIntMask) == rdbLpEncoding64BitInt)
}
func lpEncodingIs32BitStr(b byte) bool {
	return (((b) & rdbLpEncoding32BitStrMask) == rdbLpEncoding32BitStr)
}
func lpEncoding6BitStrLen(b []byte) uint32 {
	return uint32(b[0] & 0x3F)
}
func lpEncoding12BitStrLen(b []byte) uint32 {
	return (uint32((b)[0]&0xF) << 8) | uint32((b)[1])
}
func lpEncoding32BitStrLen(b []byte) uint32 {
	return (uint32(b[1]) << 0) |
		(uint32(b[2]) << 8) |
		(uint32(b[3]) << 16) |
		(uint32(b[4]) << 24)
}

func (d *rdbDecode) readZiplist(key []byte, expiry int64, addListEvents bool) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	if addListEvents {
		d.event.BeginList(key, length, expiry)
	}
	for i := int64(0); i < length; i++ {
		entry, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Rpush(key, entry)
	}
	if addListEvents {
		d.event.EndList(key)
	}
	return nil
}

func (d *rdbDecode) readZiplistZset(key []byte, expiry int64) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	cardinality, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2
	d.event.BeginZSet(key, cardinality, expiry)
	for i := int64(0); i < cardinality; i++ {
		member, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		d.event.Zadd(key, score, member)
	}
	d.event.EndZSet(key)
	return nil
}

func (d *rdbDecode) readZiplistHash(key []byte, expiry int64) error {
	ziplist, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(ziplist)
	length, err := readZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2
	d.event.BeginHash(key, length, expiry)
	for i := int64(0); i < length; i++ {
		field, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := readZiplistEntry(buf)
		if err != nil {
			return err
		}
		d.event.Hset(key, field, value)
	}
	d.event.EndHash(key)
	return nil
}

func readZiplistLength(buf *sliceBuffer) (int64, error) {
	buf.Seek(8, 0) // skip the zlbytes and zltail
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func readZiplistEntry(buf *sliceBuffer) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == 254 {
		buf.Seek(4, 1) // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == rdbZiplist6bitlenString:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == rdbZiplist14bitlenString:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == rdbZiplist32bitlenString:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header == rdbZiplistInt16:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == rdbZiplistInt32:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == rdbZiplistInt64:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	case header == rdbZiplistInt24:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header == rdbZiplistInt8:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header>>4 == rdbZiplistInt4:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	}

	return nil, fmt.Errorf("rdb: unknown ziplist header byte: %d", header)
}

func (d *rdbDecode) readIntset(key []byte, expiry int64) error {
	intset, err := d.readString()
	if err != nil {
		return err
	}
	buf := newSliceBuffer(intset)
	intSizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(intSizeBytes)

	if intSize != 2 && intSize != 4 && intSize != 8 {
		return fmt.Errorf("rdb: unknown intset encoding: %d", intSize)
	}

	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)

	d.event.BeginSet(key, int64(cardinality), expiry)
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		d.event.Sadd(key, []byte(intString))
	}
	d.event.EndSet(key)
	return nil
}

func skipLine(r io.Reader) error {
	br := bufio.NewReader(r)
	_, _, err := br.ReadLine()
	if err != nil {
		return fmt.Errorf("rdb: invalid file format,skip bulk string error.")
	}
	return nil
}

func (d *rdbDecode) checkHeader() error {
	err := skipLine(d.r)
	if err != nil {
		return err
	}
	header := make([]byte, 9)
	_, err = io.ReadFull(d.r, header)
	if err != nil {
		return err
	}
	ind := bytes.Index(header, []byte("R"))
	if ind < 0 {
		return fmt.Errorf("rdb: invalid file format,header no contain REDIS magic %s.", header)
	}
	if ind > 0 {
		tmp := make([]byte, ind)
		_, err := io.ReadFull(d.r, tmp)
		if err != nil {
			return err
		}
		header = append(header[ind:], tmp...)
	}

	if !bytes.Equal(header[:5], []byte("REDIS")) {
		return fmt.Errorf("rdb: invalid file format,header %s", header)
	}

	version, _ := strconv.ParseInt(string(header[5:]), 10, 64)
	if version < 1 || version > rdbVersion {
		return fmt.Errorf("rdb: invalid RDB version number %d", version)
	}

	return nil
}

func (d *rdbDecode) readString() ([]byte, error) {
	length, encoded, err := d.readLength()
	if err != nil {
		return nil, err
	}
	if encoded {
		switch length {
		case rdbEncInt8:
			i, err := d.readUint8()
			return []byte(strconv.FormatInt(int64(int8(i)), 10)), err
		case rdbEncInt16:
			i, err := d.readUint16()
			return []byte(strconv.FormatInt(int64(int16(i)), 10)), err
		case rdbEncInt32:
			i, err := d.readUint32()
			return []byte(strconv.FormatInt(int64(int32(i)), 10)), err
		case rdbEncLZF:
			clen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			ulen, _, err := d.readLength()
			if err != nil {
				return nil, err
			}
			compressed := make([]byte, clen)
			_, err = io.ReadFull(d.r, compressed)
			if err != nil {
				return nil, err
			}
			decompressed := lzfDecompress(compressed, int(ulen))
			if len(decompressed) != int(ulen) {
				return nil,
					fmt.Errorf("decompressed string length %d didn't match expected length %d",
						len(decompressed),
						ulen,
					)
			}
			return decompressed, nil
		}
	}

	str := make([]byte, length)
	_, err = io.ReadFull(d.r, str)
	return str, errors.Wrap(err, "readfailed")
}

func (d *rdbDecode) readUint8() (uint8, error) {
	b, err := d.r.ReadByte()
	return uint8(b), errors.Wrap(err, "readfailed")
}

func (d *rdbDecode) readUint16() (uint16, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:2])
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.LittleEndian.Uint16(d.intBuf), nil
}

func (d *rdbDecode) readUint32() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.LittleEndian.Uint32(d.intBuf), nil
}

func (d *rdbDecode) readUint64() (uint64, error) {
	_, err := io.ReadFull(d.r, d.intBuf)
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.LittleEndian.Uint64(d.intBuf), nil
}

func (d *rdbDecode) readUint32Big() (uint32, error) {
	_, err := io.ReadFull(d.r, d.intBuf[:4])
	if err != nil {
		return 0, errors.Wrap(err, "readfailed")
	}
	return binary.BigEndian.Uint32(d.intBuf), nil
}

func (d *rdbDecode) readBinaryFloat64() (float64, error) {
	floatBytes := make([]byte, 8)
	_, err := io.ReadFull(d.r, floatBytes)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(floatBytes)), nil
}

// Doubles are saved as strings prefixed by an unsigned
// 8 bit integer specifying the length of the representation.
// This 8 bit integer has special values in order to specify the following
// conditions:
// 253: not a number
// 254: + inf
// 255: - inf
func (d *rdbDecode) readFloat64() (float64, error) {
	length, err := d.readUint8()
	if err != nil {
		return 0, err
	}
	switch length {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		floatBytes := make([]byte, length)
		_, err := io.ReadFull(d.r, floatBytes)
		if err != nil {
			return 0, err
		}
		f, err := strconv.ParseFloat(string(floatBytes), 64)
		return f, err
	}
}

func (d *rdbDecode) readLength() (uint64, bool, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, false, errors.Wrap(err, "readfailed")
	}
	// The first two bits of the first byte are used to indicate the length encoding type
	switch (b & 0xc0) >> 6 {
	case rdb6bitLen:
		// When the first two bits are 00, the next 6 bits are the length.
		return uint64(b & 0x3f), false, nil
	case rdb14bitLen:
		// When the first two bits are 01, the next 14 bits are the length.
		bb, err := d.r.ReadByte()
		if err != nil {
			return 0, false, errors.Wrap(err, "readfailed")
		}
		return (uint64(b&0x3f) << 8) | uint64(bb), false, nil
	case rdb32bitLen:
		bb, err := d.readUint32()
		if err != nil {
			return 0, false, err
		}
		return uint64(bb), false, nil
	case rdb64bitLen:
		bb, err := d.readUint64()
		if err != nil {
			return 0, false, err
		}
		return bb, false, nil
	case rdbEncVal:
		// When the first two bits are 11, the next object is encoded.
		// The next 6 bits indicate the encoding type.
		return uint64(b & 0x3f), true, nil
	default:
		// When the first two bits are 10, the next 6 bits are discarded.
		// The next 4 bytes are the length.
		length, err := d.readUint32Big()
		return uint64(length), false, err
	}

}

func verifyDump(d []byte) error {
	if len(d) < 10 {
		return fmt.Errorf("rdb: invalid dump length")
	}
	version := binary.LittleEndian.Uint16(d[len(d)-10:])
	if version > uint16(rdbVersion) {
		return fmt.Errorf("rdb: invalid version %d, expecting %d", version, rdbVersion)
	}

	if binary.LittleEndian.Uint64(d[len(d)-8:]) != Digest(d[:len(d)-8]) {
		return fmt.Errorf("rdb: invalid CRC checksum")
	}

	return nil
}

func lzfDecompress(in []byte, outlen int) []byte {
	out := make([]byte, outlen)
	for i, o := 0, 0; i < len(in); {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	return out
}
