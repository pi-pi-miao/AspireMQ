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
	"sync"
)

var (
	mq  *AspireMQ
)

type AspireMQ struct{
	Addr string
	stopServer chan struct{}
	rwLock   *sync.RWMutex
	Node     *safe_map.SyncMap
	report   *safe_map.SyncMap
}

type aspireMQReport struct {
	id   string
	conn net.Conn
	aspireMQ *AspireMQ
	getConn  chan []byte
	sendConn chan []byte
	stopConn chan struct{}
}

// make sure mq.init work
func AspireMQServer(addr string)error{
	mq = &AspireMQ{
		Addr:addr,
		rwLock:&sync.RWMutex{},
		stopServer:make(chan struct{}),
		Node:safe_map.New(),
		report:safe_map.New()}
	mq.init()
	// todo add log
	fmt.Println("AspireMQ is finish ... ")
	mq.close()
	return nil
}

func (a *AspireMQ)init(){
	common.InitCommon()
	aspire_consumer.Init()
	fmt.Printf("AspireMQ is running and addr is %v ",a.Addr)
	l,err := net.Listen("tcp",a.Addr)
	if err != nil {
		return
	}
	for {
		select {
		case <- a.stopServer:
			return
		default:
		}
		conn,err := l.Accept()
		if err != nil {
			// todo report and add log
			return
		}
		id, ok := <-common.MessageId
		if !ok {
			// todo add log about this aspire closed
			return
		}
		// todo 删除
		fmt.Println("[AspireMQ.init] accept new conn")
		report := newAspireMQReport()
		report.conn = conn
		report.aspireMQ = a
		// todo report this
		a.report.Set(id,report)
		report.dispatcher()
	}
	return
}

func newAspireMQReport()*aspireMQReport{
	return &aspireMQReport{
		getConn:make(chan []byte,10240),
		sendConn:make(chan []byte,10240),
		stopConn:make(chan struct{}),
	}
}

func (a *aspireMQReport)dispatcher(){
	wrapper.Wrapper(a.get,"mq.get")
	wrapper.Wrapper(a.read,"mq.read")
}

func (a *aspireMQReport)get(){
	for data := range a.getConn {
		message := &types.Message{}
		productMessage := &types.OurSelf{}
		if err := proto.Unmarshal(data,message);err != nil {
			// todo get log and report
			a.close()
			return
		}
		switch message.Type {
		case types.MESSAGEOURSELFTYPE:
			// todo debug so 删除
			fmt.Println("[ aspireMQ.get ]",string(message.Data),"type",message.Type)
			if err := proto.Unmarshal(message.Data,productMessage);err != nil {
				// todo get log and report Illegal request code
				a.close()
				return
			}
			aspire := aspireMQ{
				conn:safe_map.New(),
				productMessage:&types.OurSelf{},
			}
			*aspire.productMessage = *productMessage
			// todo report dashboard，if heartbeat err delete
			mq.Node.Set(productMessage.Addr,aspire)
			fmt.Println("1")
			aspire.create()
		case types.MESSAGECONSUMER:
			group := aspire_consumer.NewTopicGroup(message.Topic)
			group.Create(string(message.Data),a.conn)
		default:
			fmt.Println("[ aspireMQ.get.default ]",string(message.Data),"type",message.Type)
			// todo get log and report
			a.close()
			return
		}
	}
}

func (a *aspireMQReport)read(){
	sizeData := make([]byte, 2)
	for {
		select {
		case <- a.stopConn:
			return
		default:
		}
		if _, err := io.ReadFull(a.conn, sizeData); err != nil {
			a.close()
			// todo 打印日志
			return
		}
		data := make([]byte, binary.LittleEndian.Uint16(sizeData))
		if _, err := io.ReadFull(a.conn, data); err != nil {
			a.close()
			// todo 打印日志
			return
		}
		a.getConn <- data
	}
}


func (a *aspireMQReport)close(){
	close(a.getConn)
	close(a.sendConn)
	close(a.stopConn)
	a.aspireMQ.report.Delete(a.id)
}

func (a *AspireMQ)close(){
	close(a.stopServer)
	common.Close()
}

