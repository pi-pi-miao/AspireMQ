package aspire_mq

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire_consumer"
	"github.com/pi-pi-miao/AspireMQ/pkg/common"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
	"io"
	"net"
)

// aspireConn manager
type aspireMQ struct {
	conn *safe_map.SyncMap
	productMessage *types.OurSelf
}

type aspireConn struct {
	id     string
	mq     *aspireMQ
	stopConn chan struct{}
	getConn chan []byte
	sendConn chan []byte
	addr string
	conn net.Conn
}

func (m *aspireMQ)create()(err error){
	// todo create number conn
	for i:=0;i<int(m.productMessage.CpuNum);i++ {
		a := &aspireConn{
			stopConn:make(chan struct{}),
			getConn:make(chan []byte,10240),
			sendConn:make(chan []byte,10240),
			addr:m.productMessage.Addr,
		}
		a.conn,err = net.Dial("tcp",a.addr)
		if err != nil {
			return err
		}
		id, ok := <-common.MessageId
		if !ok {
			// todo add log about this aspire closed
			return
		}
		a.id = id
		// todo report all conn
		m.conn.Set(id,a)
		wrapper.Wrapper(a.get,"a.get")
		wrapper.Wrapper(a.read,"a.read")
		wrapper.Wrapper(a.write,"a.write")
	}
	return nil
}

func (a *aspireConn)get(){
	for data := range a.getConn {
		m := &types.Message{}
		err := proto.Unmarshal(data,m)
		if err != nil {
			fmt.Printf("unmarshal m err %v",err)
			return
		}
		fmt.Printf("get from aspire product topic is %v m is %v \n",m.Topic,m)
		aspire_consumer.Dispatcher(m.Topic,m.Data)
	}
}

func (a *aspireConn)read(){
	sizeData := make([]byte,2)
	for {
		select {
		case <- a.stopConn:
			return
		default:
		}
		if _,err := io.ReadFull(a.conn,sizeData);err != nil {
			// todo get log
			a.close()
			return
		}
		data := make([]byte,binary.LittleEndian.Uint16(sizeData))
		if _,err := io.ReadFull(a.conn,data);err != nil {
			// todo get log
			a.close()
			return
		}
		a.getConn <- data
	}
}

func (a *aspireConn)write(){
}

func (a *aspireConn)close(){
	close(a.stopConn)
	close(a.getConn)
	close(a.sendConn)
	a.conn.Close()
	a.mq.conn.Delete(a.id)
}