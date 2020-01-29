package main

import (
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
	aspire_mq.AspireMQServer(cli.String("addr"))
	return nil
}

func reloads(cli *cli.Context)error{
	return nil
}
