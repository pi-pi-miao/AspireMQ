package aspire_mq

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire_consumer"
	"github.com/pi-pi-miao/AspireMQ/pkg/common"
	"github.com/pi-pi-miao/AspireMQ/pkg/logger"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
	"io"
	"net"
	"sync"
	"time"
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
	once *sync.Once
}

func (m *aspireMQ)create()(err error){
	// todo create number conn
	for i:=0;i<int(m.productMessage.CpuNum);i++ {
		a := &aspireConn{
			mq:m,
			stopConn:make(chan struct{}),
			getConn:make(chan []byte,10240),
			sendConn:make(chan []byte,10240),
			addr:m.productMessage.Addr,
			once:&sync.Once{},
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
	}
	return nil
}

func (a *aspireConn)get(){
	for data := range a.getConn {
		m := &types.Message{}
		err := proto.Unmarshal(data,m)
		if err != nil {
			logger.Logger.Error("[aspireConn.get][%v] unmarshal data %v err %v",time.Now(),string(data),err)
			return
		}
		aspire_consumer.Dispatcher(m.Topic,m)
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
			logger.Logger.Error("[aspireConn.read][%v] read conn close err %v",time.Now(),err)
			a.close()
			return
		}
		data := make([]byte,binary.LittleEndian.Uint16(sizeData))
		if _,err := io.ReadFull(a.conn,data);err != nil {
			logger.Logger.Error("[aspireConn.read][%v] read conn close err %v",time.Now(),err)
			a.close()
			return
		}
		fmt.Println("[aspireConn] data is  ",string(data))
		a.getConn <- data
	}
}

func (a *aspireConn)close(){
	a.once.Do(
		func() {
			close(a.stopConn)
			close(a.getConn)
			close(a.sendConn)
			if a.conn != nil {
				a.conn.Close()
			}
			a.mq.conn.Delete(a.id)
		})
}