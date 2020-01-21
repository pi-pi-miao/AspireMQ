package aspire

import (
	"AspireMQ/api/types"
	"AspireMQ/pkg/common"
	"AspireMQ/staging/src/aspire.mq/wrapper"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"net"
	"sync"
	"time"
	"AspireMQ/staging/src/safe_map"
)

var (
	Mq *AspireMQ
	a  *aspire
)

// all manager
type AspireMQ struct {
	Conn *safe_map.SyncMap
}

type aspire struct {
	key string
	conn net.Conn
	aspireMq *AspireMQ
	group string
	getConn chan []byte
	getConnFlag bool
	stop    chan bool
	once   *sync.Once
	lock   *sync.RWMutex
	gLock  *sync.RWMutex
}

func NewAspireMQ() {
	Mq = &AspireMQ{
		Conn: safe_map.New(),
	}
	a = &aspire{
		getConn:make(chan []byte,1000),
		stop:make(chan bool),
		aspireMq:Mq,
		once:&sync.Once{},
		lock:&sync.RWMutex{},
		gLock:&sync.RWMutex{},
	}
	return
}

func GetConn(conn net.Conn, key string) {
	a.conn = conn
	a.key  = key
	Mq.Conn.Set(key, a)
	wrapper.Wrapper(a.get,"aspire.[get]")
	wrapper.Wrapper(a.read,"aspire.[read]")
	wrapper.Wrapper(a.send, "aspire.[send]")
}

//*
// send message abnormal add this message to cache
//*/
func (g *aspire) send() {
	for v := range common.SendMessage {
		g.gLock.RLock()
		if v.Group == g.group {
			sendData,err := proto.Marshal(&types.Message{
				Type:                 v.Type,
				Data:                 v.Data,
				Group:                v.Group,
			})
			if err != nil {
				// todo 打印日志，报警处理
			}
			data := make([]byte, 2)
			binary.LittleEndian.PutUint16(data, uint16(len(sendData)))
			data = append(data, sendData...)
			if _, err := g.conn.Write(data); err != nil {
				// todo 打印这条消息到日志并且报警
				fmt.Println("write err", err)
				common.TemporaryCache.Set(fmt.Sprintf("%v", time.Now()), v)
			}
			g.gLock.RUnlock()
		}else {
			// 出现这样的情况很大原因是aspireMQ服务挂掉了,或者修改了group
			// todo 打印日志消息,把消息存储到日志中
			fmt.Println("this message is not group can receive or aspireMQ server is abnormal")
		}
	}
}

func (g *aspire)read(){
	sizeData := make([]byte, 2)
	for {
		select {
		case <-g.stop:
			return
		default:
		}
		g.lock.RLock()
		if g.getConnFlag {
			if _, err := io.ReadFull(g.conn, sizeData); err != nil {
				// todo 待添加日志
				g.close()
				return
			}
			data := make([]byte, binary.LittleEndian.Uint16(sizeData))
			if _, err := io.ReadFull(g.conn, data); err != nil {
				// todo 待添加日志
				g.close()
				return
			}
			g.getConn <- data
			g.lock.RUnlock()
			continue
		}
		g.lock.RUnlock()
	}
}

// aspireMQ has register group api and can edit group
// receive group
func (g *aspire)get(){
	receiveData := &types.Message{}
	for v := range g.getConn {
		err := proto.Unmarshal(v,receiveData)
		if err != nil {
			// todo aspireMQ service is abnormal  待打印日志
		}
		g.gLock.Lock()
		g.group = receiveData.Group
		g.gLock.Unlock()
	}
}

func (g *aspire)close(){
	g.once.Do(func() {
		g.lock.Lock()
		g.getConnFlag = false
		close(g.getConn)
		g.lock.Unlock()
		close(g.stop)
		g.conn.Close()
		g.aspireMq.Conn.Delete(g.key)
	})
}
