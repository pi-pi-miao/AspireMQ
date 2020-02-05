package getData

import (
	"encoding/binary"
	"errors"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
	"io"
	"net"
	"sync"
)

var (
   Mq *AspireMQ
   Message = make(chan []byte,10240)
)

type AspireMQ struct {
	Addr  []string
	Addrs *safe_map.SyncMap      // key is addr value is struct{}
	aspireConn *safe_map.SyncMap // key is addr value is aspireConn
}

type aspireConn struct {
	aspireMQ *AspireMQ
	once   *sync.Once
	stopConn chan struct{}
	conn net.Conn
	addr string
}

func NewAspireMQ(){
	Mq = &AspireMQ{
		Addrs:safe_map.New(),
	}
}

func DelData(addr []string){
	for k,_ := range addr{
		address := addr[k]
		if value,ok := Mq.Addrs.Get(address);ok {
			value.(*aspireConn).close()
			Mq.Addrs.Delete(address)
		}
	}

}

func SetData(addr []string)error{
	for k,_ := range addr {
		address := addr[k]
		if _,ok:= Mq.Addrs.Get(address);ok{
			return errors.New("addr is already exist")
		}
		conn,err := net.Dial("tcp",address)
		if err != nil {
			return err
		}
		a := &aspireConn{
			aspireMQ:Mq,
			once:&sync.Once{},
			stopConn:make(chan struct{}),
		}
		Mq.Addrs.Set(addr[k],a)
		a.conn = conn
		wrapper.Wrapper(a.read,"aspireConn.read")
	}
	return nil
}

func (a *aspireConn)read(){
	data := make([]byte,2)
	for {
		select {
		case <- a.stopConn:
			return
		default:
		}
		if _,err := io.ReadFull(a.conn,data);err != nil {
			a.close()
			return
		}
		d := make([]byte,binary.LittleEndian.Uint16(data))
		if _,err := io.ReadFull(a.conn,d);err != nil {
			a.close()
			return
		}
		Message <- d
	}
}

func (a *aspireConn)close(){
	a.once.Do(
		func() {
			a.conn.Close()
			close(a.stopConn)
		})
}