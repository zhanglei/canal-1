package canal

import (
	"strings"

	"github.com/pkg/errors"
)

// Command  all the command combinations
type Command struct {
	T CommandType
	D []string
}

func (c *Command) String() string {
	return strings.Join(c.D, " ")
}

func (c *Command) Type() CommandType {
	cmdType, exists := CommandTypeMap[c.D[0]]
	if !exists {
		return Undefined
	}
	return cmdType
}

func (c *Command) CommandName() string {
	return c.D[0]
}

func (c *Command) Args() []interface{} {
	args := make([]interface{}, len(c.D)-1, len(c.D)-1)
	for i := range c.D[1:] {
		args[i] = c.D[i+1]
	}
	return args
}

func buildStrCommand(s string) []string {
	return strings.Split(s, " ")
}

func NewCommand(args ...string) (*Command, error) {
	if len(args) == 0 {
		return nil, errors.New("Empty args.")
	}
	return &Command{D: args}, nil
}
