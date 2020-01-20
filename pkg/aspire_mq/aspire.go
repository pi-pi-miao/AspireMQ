package aspire_mq

import (
	"AspireMQ/staging/src/aspire.mq/wrapper"
	"fmt"
	"net"
)

type AspireDb struct {
	Aspire []*aspireDb
}

type aspireDb struct {
	conn net.Conn
	err  error
}

// init
func Aspire(ip,port string)*aspireDb{
	a := &aspireDb{}
	addr := fmt.Sprintf("%v:%v",ip,port)
	wrapper.Wrapper(
		func() {
			ln,err := net.Listen("tcp",addr)
			if err != nil {
				// todo 打印日志
				a.err = err
				return
			}
			conn,err := ln.Accept()
			if err != nil {
				//todo 打印日志
				return
			}
			a.conn = conn
		},"Aspire",
		)
	return a
}

// product sync to consumer
func (a *aspireDb)Do(){

}
