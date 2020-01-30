package main

import (
	"errors"
	"fmt"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire_mq"
	"github.com/urfave/cli" // 2.1.1
)

var (
	start = &cli.Command{
		Name:                   "start",
		Usage:					"--addr=0.0.0.0:10001",
		Description:            "start AspireMQ",
		Action:                 starts,
		Flags:                  []cli.Flag{
			&cli.StringFlag{
				Name:"addr",
				Usage:"this AspireMQ addr",
			},
		},
	}
	reload = &cli.Command{
		Name:                   "reload",
		Usage:                  "",
		Description:            "reload AspireMQ",
		Action:                 reloads,
		Flags:                  []cli.Flag{
			&cli.StringFlag{
				Name:"",
				Usage:"",
			},
		},
	}
)

func starts(cli *cli.Context)error{
	addr := cli.String("addr")
	if addr == "" {
		return errors.New("please add --addr=ip:port")
	}
	if err := aspire_mq.AspireMQServer(addr);err != nil {
		fmt.Println("run aspire failed ",err)
		return err
	}
	return nil
}

func reloads(cli *cli.Context)error{
	return nil
}
