package aspire_mq

import (
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire"
	"github.com/pi-pi-miao/AspireMQ/pkg/client"
	"github.com/pi-pi-miao/AspireMQ/pkg/common"
	"github.com/pi-pi-miao/AspireMQ/pkg/logger"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"time"
)

// todo 添加plugin

type AspireMq struct {
	Addr   string
	Err    error
	SendCh chan string
	stopServer chan struct{}
	Url    string
}

// start Call this function
func Aspire(ip, port string, originAddr []string) (*AspireMq, error) {
	a := &AspireMq{
		Addr: fmt.Sprintf("%v:%v", ip, port),
		stopServer:make(chan struct{}),
	}
	for k,_ := range originAddr {
		if a.Addr == originAddr[k] {
			return nil,errors.New("this origin addr is not right")
		}
	}
	aspire.NewAspireMQ()
	common.InitCommon()
	a.InitLog("")
	if _, err := a.Init(originAddr); err != nil {
		return nil, err
	}
	return a, nil
}

// todo 待加工
func (a *AspireMq)InitLog(url string){
	logger.New(url,"debug","./src/github.com/pi-pi-miao/AspireMQ/log/",0)
}

func (a *AspireMq) Init(addr []string) (*AspireMq, error) {
	logger.Logger.Debug("[AspireMq][%v] start success",time.Now())
	wrapper.Wrapper(
		func() {
			ln, err := net.Listen("tcp", a.Addr)
			if err != nil {
				logger.Logger.Error("[AspireMq.Init][%v] listen err %v",time.Now(),err)
				a.Err = err
				return
			}
			for {
				select {
				case <- a.stopServer:
					return
				default:
				}
				conn, err := ln.Accept()
				if err != nil {
					logger.Logger.Error("[AspireMq.Init][%v] Accept err %v",time.Now(),err)
					return
				}
				id, ok := <-common.MessageId
				if !ok {
					// todo add log about this aspire closed
					logger.Logger.Error("[AspireMq.Init][%v] common.MessageId close %v",time.Now(),"aspire product finish")
					return
				}
				aspire.GetConn(conn, id)
			}
		}, "Aspire",
	)
	if err := client.ReportOurSelft(a.Addr, addr); err != nil {
		return nil, err
	}
	if a.Err != nil {
		return nil, a.Err
	}
	// make sure init success
	time.Sleep(10 * time.Millisecond)
	return a, nil
}

//**
// close common.MessageId
//
//*/

// product sync to consumer
func (a *AspireMq) Publish(topic, message string) error {
	id, ok := <-common.MessageId
	if !ok {
		return errors.New("Unexpected mistakes about get message id")
	}
	d, err := proto.Marshal(&types.Product{
		Message: message,
		Id:      id,
	})
	if err != nil {
		return err
	}
	if common.SendMessageFlag {
		common.SendMessage <- types.NewMessages(types.MESSAGETYPE,topic,d)
	}
	return nil
}

// todo request/reply
func (a *AspireMq)RequestPublish(topic,message string)error{
	return nil
}

// todo queue publish
func (a *AspireMq)QueuePublish(topic,message string)error{
	return nil
}

// todo timer task publish
func (a *AspireMq)TimerTaskPublish(topic,message string)error {
	return nil
}

func (a *AspireMq)close(){
	close(a.stopServer)
}

//*
//this is group
//*/

type GroupAspireMq struct {

}

// group call this
func GroupAspire()error{
	return nil
}

// todo group publish
func (a *GroupAspireMq)GroupPublish(group,topic,message string)error{
	return nil
}

// todo group queue publish
func (a *GroupAspireMq)GroupQueuePublish(group,message string)error{
	return nil
}

// todo group timer task publish
func (a *GroupAspireMq)GroupTimerTaskPublish(group,topic,message string)error {
	return nil
}

// todo group request reply publish
func (a *GroupAspireMq)GroupRequestPublish(group,topic,message string)error {
	return nil
}