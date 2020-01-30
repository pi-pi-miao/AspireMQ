package main

import (
	"fmt"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire_mq"
	"time"
)

// aspireMQ server Addr
var originAddr = []string{
	"127.0.0.1:8081",
}

func main(){
	aspireMQ,err := aspire_mq.Aspire("127.0.0.1","8080",originAddr)
	if err != nil {
		fmt.Println("conn aspire err",err)
		return
	}
	aspireMQ.Publish("aspire","hello")
	time.Sleep(10*time.Second)
}
