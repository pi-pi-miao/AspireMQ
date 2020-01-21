package aspire_mq

import (
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire"
	"github.com/pi-pi-miao/AspireMQ/pkg/client"
	"github.com/pi-pi-miao/AspireMQ/pkg/common"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"time"
)

// todo 添加plugin

type SyncAspireMq struct {
	Addr   string
	Err    error
	SendCh chan string
}

// start Call this function
func SyncAspireRegisterOurself(ip, port, originIp, originPort string) (*SyncAspireMq, error) {
	a := &SyncAspireMq{
		Addr: fmt.Sprintf("%v:%v", ip, port),
	}
	aspire.NewAspireMQ()
	common.InitCommon()
	if _, err := a.Init(fmt.Sprintf("%v:%v", originIp, originPort)); err != nil {
		return nil, err
	}

	return a, nil
}

func (a *SyncAspireMq) Init(addr string) (*SyncAspireMq, error) {
	wrapper.Wrapper(
		func() {
			ln, err := net.Listen("tcp", a.Addr)
			if err != nil {
				// todo 打印日志
				a.Err = err
				return
			}
			conn, err := ln.Accept()
			if err != nil {
				//todo 打印日志
				return
			}
			id, ok := <-common.MessageId
			if !ok {
				// 代表这个程序关闭了// 打印日志
				return
			}
			aspire.GetConn(conn, id)
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
func (a *SyncAspireMq) Do(topic, message,group string) error {
	id, ok := <-common.MessageId
	if !ok {
		return errors.New("Unexpected mistakes about get message id")
	}
	d, err := proto.Marshal(&types.Product{
		Topic:   topic,
		Message: message,
		Id:      id,
	})
	if err != nil {
		return err
	}
	m := types.NewMessages()
	m.Data = d
	m.Group = group
	m.Type = types.MESSAGETYPE
	if common.SendMessageFlag {
		common.SendMessage <- m
	}
	return nil
}
