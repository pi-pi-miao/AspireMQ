package aspire_consumer

import (
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
	"net"
	"sync"
)

var (
	topicProcessor = safe_map.New()
	work           = make(chan task, 100)
	number         = 100
)

type (
	topicGroup struct {
		topic     string
		processor []processor
		lock      *sync.RWMutex
	}
	processor struct {
		t         *topicGroup
		conn      net.Conn
		heartbeat net.Conn
	}
	task struct {
		f    func(data []byte)
		data []byte
	}
)

func NewTopicGroup(topic string) *topicGroup {
	return &topicGroup{
		processor: make([]processor, 0, 1024),
		lock:      &sync.RWMutex{},
		topic:     topic,
	}
}

func (t *topicGroup) Create(addr string, conn net.Conn) {
	p := &processor{
		t:         t,
		heartbeat: conn,
	}
	p.conn, _ = net.Dial("tcp", addr)
}

func Init() {
	for i := 0; i < number; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					// todo add log
				}
			}()
			for v := range work {
				v.f(v.data)
			}
		}()
	}
}

func (p *processor) send(data []byte) {
	if _, err := p.conn.Write(data); err != nil {
		// todo get log report and conn again three time
		return
	}
}

// todo 待定
func (p *processor) close() {
	p.conn.Close()
	p.heartbeat.Close()
}

func Dispatcher(topic string, data []byte) {
	processor, ok := topicProcessor.Get(topic)
	if !ok {
		// todo 持久化这条数据
		return
	}
	t := task{
		data: data,
	}
	group := processor.(topicGroup)
	group.lock.RLock()
	for k, _ := range group.processor {
		t.f = group.processor[k].send
		work <- t
	}
	group.lock.RUnlock()
}
