package aspire

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pi-pi-miao/AspireMQ/api/types"
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
	Mq *AspireMQ
)

// all manager
type AspireMQ struct {
	Conn *safe_map.SyncMap
}

type aspire struct {
	key         string
	conn        net.Conn
	aspireMq    *AspireMQ
	group       string
	getConn     chan []byte
	stop        chan bool
	once        *sync.Once
}

func NewAspireMQ() {
	Mq = &AspireMQ{
		Conn: safe_map.New(),
	}
	return
}

func GetConn(conn net.Conn, key string) {
	engine(&aspire{
		getConn:  make(chan []byte, 1000),
		stop:     make(chan bool),
		aspireMq: Mq,
		once:     &sync.Once{},
		conn:conn,
		key:key,
	})
}

// todo add heartbeat
func engine(a *aspire){
	Mq.Conn.Set(a.key, a)
	wrapper.Wrapper(a.read, "aspire.[read]")
	wrapper.Wrapper(a.send, "aspire.[send]")
}

//*
// send message abnormal add this message to cache
//*/
func (g *aspire) send() {
	for v := range common.SendMessage {
		sendData, err := proto.Marshal(&types.Message{
			Type:  v.Type,
			Data:  v.Data,
			Topic:v.Topic,
		})
		if err != nil {
			logger.Logger.Error("[aspire.send][%v] marshal data %v err %v",time.Now(),string(sendData),err)
			return
		}
		data := make([]byte, 2)
		binary.LittleEndian.PutUint16(data, uint16(len(sendData)))
		data = append(data, sendData...)
		if _, err := g.conn.Write(data); err != nil {
			// todo 打印这条消息到日志并且报警
			fmt.Println("write err", err)
			common.TemporaryCache.Set(fmt.Sprintf("%v", time.Now()), v)
		}
	}
}

func (g *aspire) read() {
	sizeData := make([]byte, 2)
	for {
		select {
		case <-g.stop:
			return
		default:
		}
		if _, err := io.ReadFull(g.conn, sizeData); err != nil {
			logger.Logger.Error("[aspire.read][%v] read conn close err %v",time.Now(),err)
			g.close()
			return
		}
		data := make([]byte, binary.LittleEndian.Uint16(sizeData))
		if _, err := io.ReadFull(g.conn, data); err != nil {
			logger.Logger.Error("[aspire.read][%v] read conn close err %v",time.Now(),err)
			g.close()
			return
		}
		g.getConn <- data
		return
	}
}

// todo heartbeat

// todo add this to group
// aspireMQ has register group api and can edit group
// receive group
//func (g *aspire) get() {
//	receiveData := &types.Message{}
//	for v := range g.getConn {
//		err := proto.Unmarshal(v, receiveData)
//		if err != nil {
//			// todo aspireMQ service is abnormal  待打印日志
//		}
//		g.gLock.Lock()
//		g.group = receiveData.Group
//		g.gLock.Unlock()
//	}
//}

func (g *aspire) close() {
	g.once.Do(func() {
		close(g.getConn)
		close(g.stop)
		g.conn.Close()
		g.aspireMq.Conn.Delete(g.key)
		Mq.Conn.Delete(g.key)
	})
}
