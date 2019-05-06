# canal
redis-canal

```go
cd $GOPATH/src
git clone https://github.com/laik/canal.git
```

```go
package main

import (
	"canal"
	"log"
	"os"
)

type printer struct{}

func (p *printer) Command(cmd *canal.Command) error {
	log.Printf("cmd=%v\n", cmd)
	return nil
}

func main() {
	log.SetOutput(os.Stdout)
	repl, err := canal.NewCanal(canal.NewConfig("127.0.0.1:6379"))
	if err != nil {
		panic(err)
	}
	defer repl.Close()

	if err := repl.Run(&printer{}); err != nil {
		panic(err)
	}
}

}
```
