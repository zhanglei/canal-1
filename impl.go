package canal

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
)

func (c *Canal) Command(cmd *Command) error {
	return c.cmder.Command(cmd)
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

func (c *Canal) BeginRDB() {
	log.Printf("[CANAL] rdb parse.\n")
}

func (c *Canal) BeginDatabase(n int) {
	c.db = n
	cmd, _ := NewCommand("SELECT", fmt.Sprintf("%d", n))
	c.Command(cmd)
}

func (c *Canal) Aux(key, value []byte) {
	if string(key) == "repl-offset" {
		i, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			panic(err)
		}
		c.set(i)
	} else if string(key) == "repl-id" {
		c.runID = string(value)
	} else {
		log.Printf("[CANAL] %s %s.\n", key, value)
	}
}

func (c *Canal) ResizeDatabase(dbSize, expiresSize uint32) {}

func (c *Canal) EndDatabase(n int) {}

func (c *Canal) Set(key, value []byte, expiry int64) {
	cmd, _ := NewCommand("SET", string(key), string(value))
	c.Command(cmd)
}

func (c *Canal) BeginHash(key []byte, length, expiry int64) {}

func (c *Canal) Hset(key, field, value []byte) {
	cmd, _ := NewCommand("HSET", string(key), string(field), string(value))
	c.Command(cmd)
}
func (c *Canal) EndHash(key []byte) {}

func (c *Canal) BeginSet(key []byte, cardinality, expiry int64) {}

func (c *Canal) Sadd(key, member []byte) {
	cmd, _ := NewCommand("SADD", string(key), string(member))
	c.Command(cmd)
}
func (c *Canal) EndSet(key []byte) {}

func (c *Canal) BeginList(key []byte, length, expiry int64) {}

func (c *Canal) Rpush(key, value []byte) {
	cmd, _ := NewCommand("RPUSH", string(key), string(value))
	c.Command(cmd)
}
func (c *Canal) EndList(key []byte) {}

func (c *Canal) BeginZSet(key []byte, cardinality, expiry int64) {}

func (c *Canal) Zadd(key []byte, score float64, member []byte) {
	cmd, _ := NewCommand("ZADD", string(key), fmt.Sprintf("%f", score), string(member))
	c.Command(cmd)
}
func (c *Canal) EndZSet(key []byte) {}

func (c *Canal) BeginStream(key []byte, cardinality, expiry int64) {}

func (c *Canal) Xadd(key, id, listpack []byte) {
	cmd, _ := NewCommand("XADD", string(key), string(id), string(listpack))
	c.Command(cmd)
}
func (c *Canal) EndStream(key []byte) {}

func (c *Canal) EndRDB() {
	log.Printf("[CANAL] end rdb parse.\n")
}
