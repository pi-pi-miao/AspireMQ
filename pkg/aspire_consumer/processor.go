package aspire_consumer

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/pkg/logger"
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
	"net"
	"sync"
	"time"
)

var (
	topicProcessor = safe_map.New()
	work           = make(chan task, 100)
	number         = 100
)

type (
	topicGroup struct {
		topic     string
		processor []*processor
		lock      *sync.RWMutex
	}
	processor struct {
		t         *topicGroup
		conn      net.Conn
		heartbeat net.Conn
	}
	task struct {
		f    func(data *types.Message)
		data *types.Message
	}
)

func NewTopicGroup(topic string) *topicGroup {
	return &topicGroup{
		processor: make([]*processor, 0, 1024),
		lock:      &sync.RWMutex{},
		topic:     topic,
	}
}

func (t *topicGroup) Create(addr string, conn net.Conn) {
	p := &processor{
		t:         t,
		heartbeat: conn,
	}
	t.lock.Lock()
	t.processor = append(t.processor,p)
	t.lock.Unlock()
	p.conn, _ = net.Dial("tcp", addr)
	topicProcessor.Set(t.topic,t)
}

func Init() {
	for i := 0; i < number; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Logger.Error("[consume init][%v] goroutine panic err %v",time.Now(),err)
				}
			}()
			for v := range work {
				v.f(v.data)
			}
		}()
	}
}

func (p *processor) send(data *types.Message) {
	m ,err := proto.Marshal(data)
	message := make([]byte,2)
	if err != nil {
		fmt.Println("marshal err",err)
		return
	}
	binary.LittleEndian.PutUint16(message,uint16(len(m)))
	message = append(message, m...)
	if _, err := p.conn.Write(message); err != nil {
		fmt.Println("1")
		// todo get log report and conn again three time
		return
	}
}

// todo 待定
func (p *processor) close() {
	p.conn.Close()
	p.heartbeat.Close()
}

func Dispatcher(topic string, data *types.Message) {
	processor, ok := topicProcessor.Get(topic)
	if !ok {
		// todo 持久化这条数据
		// 支持外部存储
		return
	}
	t := task{
		data: data,
	}
	group := processor.(*topicGroup)
	group.lock.RLock()
	for k, _ := range group.processor {
		t.f = group.processor[k].send
		work <- t
	}
	group.lock.RUnlock()
}
