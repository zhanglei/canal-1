package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func main() {
	fd, err := os.Open("/tmp/appendonly7001-1.aof")
	if err != nil {
		panic(err)
	}
	nfd, err := os.OpenFile("/tmp/new.RDB", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	rd := bufio.NewReader(fd)
	for {
		str, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		nfd.WriteString(fmt.Sprintf("%q", str))
	}

}
