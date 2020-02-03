package main

import (
	"fmt"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire_consumer"
)

var (
	aspireAddr = []string{
		"127.0.0.1:8081",
	}
	cusumerAddr = "127.0.0.1:8082"
)

func main(){
	consumer := aspire_consumer.NewConsumer(cusumerAddr,aspireAddr)
	consumer.AspireConsumer()
	consumer.Register("aspire")

	data,err := consumer.Consume()
	if err != nil {
		fmt.Println("consumer failed",err)
		return
	}
	fmt.Println("consume data is ",data)
}
