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
	mq.initLog("")

	mq.init()
	logger.Logger.Error("AspireMq is finish")
	fmt.Println("AspireMQ is finish ... ")
	mq.close()
	return nil
}

// todo 待加工
func (a *AspireMQ)initLog(url string){
	logger.New(url,"debug","./src/github.com/pi-pi-miao/AspireMQ/log/",0)
}

func (a *AspireMQ)init(){
	common.InitCommon()
	aspire_consumer.Init()
	logger.Logger.Debug("[%v] AspireMQ is running and addr is %v ",time.Now(),a.Addr)
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
			logger.Logger.Error("[aspireMQReport.get][%v] unmarshal data %v err %v",time.Now(),string(data),err)
			a.close()
			return
		}
		switch message.Type {
		case types.MESSAGEOURSELFTYPE:
			if err := proto.Unmarshal(message.Data,productMessage);err != nil {
				logger.Logger.Error("[aspireMQReport.get][%v] unmarshal message.Data %v err %v",time.Now(),string(message.Data),err)
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
			logger.Logger.Error("[aspireMQReport.read][%v] read conn close err %v",time.Now(),err)
			return
		}
		data := make([]byte, binary.LittleEndian.Uint16(sizeData))
		if _, err := io.ReadFull(a.conn, data); err != nil {
			a.close()
			logger.Logger.Error("[aspireMQReport.read][%v] read conn close err %v",time.Now(),err)
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

