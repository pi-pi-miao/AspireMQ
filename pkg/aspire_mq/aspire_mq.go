package aspire_mq

import (
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"net"
	"sync"
)

type AspireMQ struct{
	Addr string
	conn net.Conn
	stopServer bool
	rwLock   *sync.RWMutex
	getConn  chan []byte
	sendConn chan []byte
}


func AspireMQServer(addr string)error{
	mq := &AspireMQ{Addr:addr,
		rwLock:&sync.RWMutex{},
	getConn:make(chan []byte,1024),
	sendConn:make(chan []byte,1024),
	stopServer:true}
	mq.init()
	wrapper.Wrapper(mq.read,"mq.read")
	wrapper.Wrapper(mq.write,"mq.write")
	return nil
}

func (a *AspireMQ)init()error{
	l,err := net.Listen("tcp",a.Addr)
	if err != nil {
		return err
	}
	for {
		a.rwLock.RLock()
		if !a.stopServer{
			a.rwLock.RUnlock()
			return nil
		}
		a.rwLock.RUnlock()
		conn,err := l.Accept()
		if err != nil {
			return err
		}
		a.conn = conn
	}
	return nil
}


func (a *AspireMQ)read(){

}

func (a *AspireMQ)write(){

}

func (a *AspireMQ)close(){
	a.rwLock.Lock()
	a.stopServer = false
	a.rwLock.Unlock()
	a.conn.Close()
	close(a.getConn)
	close(a.sendConn)
}
