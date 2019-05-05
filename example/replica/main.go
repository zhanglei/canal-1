package main

import (
	canal "canal"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
)

type nopE struct {
	canal.Nop
	db int
	o  *O
	t  *log.Logger
}

func (d *nopE) BeginRDB() {
	d.t.Printf("begin rdb parse.\n")
}
func (d *nopE) BeginDatabase(n int) {
	d.db = n
	d.t.Printf("begin select db %d.\n", n)
}
func (d *nopE) Aux(key, value []byte) {
	if string(key) == "repl-offset" {
		i, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			panic(err)
		}
		d.o.set(i)
	}
	d.t.Printf("aux key=%s value=%s db=%d.\n", key, value, d.db)
}

func (d *nopE) ResizeDatabase(dbSize, expiresSize uint32) {
	d.t.Printf("resize db=%d expiresSize=%d db=%d.\n", dbSize, expiresSize, d.db)
}

func (d *nopE) EndDatabase(n int) {
	d.t.Printf("end select db %d.\n", n)
}

func (d *nopE) EndRDB() {
	d.t.Printf("end RDB.\n")
}
func (d *nopE) Set(key, value []byte, expiry int64) {
	d.t.Printf("set key=%s value=%s db=%d.\n", key, value, d.db)
}

func (d *nopE) BeginHash(key []byte, length, expiry int64) {
	d.t.Printf("beginHash key=%s length=%d expiry=%d db=%d.\n", key, length, expiry, d.db)
}
func (d *nopE) Hset(key, field, value []byte) {
	d.t.Printf("Hset key=%s field=%s value=%s db=%d.\n", key, field, value, d.db)
}
func (d *nopE) EndHash(key []byte) {
	d.t.Printf("endHash key=%s db=%d.\n", key, d.db)
}

func (d *nopE) BeginSet(key []byte, cardinality, expiry int64) {
	d.t.Printf("BeginSet key=%s cardinality=%d expiry=%d db=%d.\n", key, cardinality, expiry, d.db)
}
func (d *nopE) Sadd(key, member []byte) {
	d.t.Printf("Sadd key=%s member=%s  db=%d.\n", key, member, d.db)
}
func (d *nopE) EndSet(key []byte) {
	d.t.Printf("EndSet key=%s db=%d.\n", key, d.db)
}

func (d *nopE) BeginList(key []byte, length, expiry int64) {
	d.t.Printf("BeginList key=%s length=%d expiry=%d db=%d.\n", key, length, expiry, d.db)
}
func (d *nopE) Rpush(key, value []byte) {
	d.t.Printf("Rpush key=%s value=%d  db=%d.\n", key, value, d.db)
}
func (d *nopE) EndList(key []byte) {
	d.t.Printf("EndList key=%s  db=%d.\n", key, d.db)
}

func (d *nopE) BeginZSet(key []byte, cardinality, expiry int64) {
	d.t.Printf("BeginZSet key=%s cardinality=%d expiry=%d db=%d.\n", key, cardinality, expiry, d.db)
}
func (d *nopE) Zadd(key []byte, score float64, member []byte) {
	d.t.Printf("Zadd key=%s score=%f member=%s db=%d.\n", key, score, member, d.db)
}
func (d *nopE) EndZSet(key []byte) {
	d.t.Printf("EndZSet key=%s db=%d.\n", key, d.db)
}

func (d *nopE) BeginStream(key []byte, cardinality, expiry int64) {
	d.t.Printf("BeginStream key=%s cardinality=%d expiry=%d db=%d.\n", key, cardinality, expiry, d.db)
}
func (d *nopE) Xadd(key, id, listpack []byte) {
	d.t.Printf("Xadd key=%s id=%s listpack=%q db=%d.\n", key, id, listpack, d.db)
}
func (d *nopE) EndStream(key []byte) {
	d.t.Printf("EndStream key=%s db=%d.\n", key, d.db)
}

type O struct {
	i int64
}

func (o *O) Increment(i int64) {
	atomic.AddInt64(&o.i, i)
}
func (o *O) Offset() string {
	return fmt.Sprintf("%d", o.i)
}
func (o *O) set(i int64) {
	atomic.StoreInt64(&o.i, i)
}

type C struct{}

func (c *C) Command(cmd *canal.Command) error {
	log.Printf("cmd=%s, args=%v\n", cmd.CommandName(), cmd.Args())
	return nil
}

func main() {
	// fromFile()
	println("------------------")
	fromClient()
}

func fromClient() {
	conn, err := net.Dial("tcp", "127.0.0.1:6380")
	if err != nil {
		panic(err)
	}
	_, err = conn.Write([]byte("*3\r\n$5\r\npsync\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		panic(err)
	}

	l := log.New(os.Stdout, "RDB: ", 0)
	o := &O{}

	// o.set(427)
	replica := canal.NewReplica(conn, &nopE{t: l, o: o}, &O{}, &C{})
	replica.SetWriter(conn)

	err = replica.DumpAndParse()
	if err != nil {
		panic(err)
	}
}

func fromFile() {
	fd, err := os.Open("/tmp/dump.aof")
	if err != nil {
		fmt.Println(err)
	}
	l := log.New(os.Stdout, "RDB: ", 0)
	c := &C{}
	o := &O{}
	replica := canal.NewReplica(fd, &nopE{t: l, o: o}, o, c)

	replica.DumpFromFile()

}
