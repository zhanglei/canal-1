package canal

import "sync"

var (
	cmdPool = sync.Pool{
		New: func() interface{} {
			return &Cmd{pairs: make(map[string][]string)}
		},
	}
	refactor = func() *Cmd {
		cmd := cmdPool.Get().(*Cmd)
		defer cmdPool.Put(cmd)
		return cmd
	}
)

type Cmd struct {
	db    int
	pairs map[string][]string
}

func (c *Cmd) beforeSelect() {

}

func (c *Cmd) getT() CommandType {
	return ""
}
