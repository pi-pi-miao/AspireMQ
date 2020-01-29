package main

import (
	"fmt"
	"github.com/urfave/cli"
	"os"
	"time"
)

func main() {
	aspired := cli.NewApp()
	aspired.Name = "AspireMQ"
	aspired.Compiled = time.Now()
	aspired.Version = VERSION
	aspired.Usage = ""
	aspired.Commands = []*cli.Command{
		start,
	}
	if err := aspired.Run(os.Args);err != nil {
		fmt.Println("this aspired start err",err)
	}
}
