package aspire_consumer

import (
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/pkg/logger"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"io"
	"net"
	"sync"
	"time"
)

var (
	once sync.Once
)

type Consumer struct {
	ConsumerAddr string
	AspireAddr   []string
	err          error
	stopServer   chan struct{}
	consumer     [2]*consumer
	consumerMessage chan []byte
}

func NewConsumer(consumeAddr string,aspireAddr []string) *Consumer {
	return &Consumer{
		stopServer: make(chan struct{}),
		consumer:   [2]*consumer{},
		consumerMessage: make(chan []byte, 10240),
		ConsumerAddr:consumeAddr,
		AspireAddr:aspireAddr,
	}
}

// consumer start
func (c *Consumer) AspireConsumer() error {
	c.Initlog("")
	if len(c.AspireAddr) == 0 && c.ConsumerAddr == "" {
		return errors.New("input aspireAddr or consumerAddr is not input")
	}
	c.init()
	if c.err != nil {
		return c.err
	}
	return nil
}

// todo 待加工
func (c *Consumer)Initlog(url string){
	logger.New(url,"debug","./src/github.com/pi-pi-miao/AspireMQ/log/",0)
}

// register topic
func (c *Consumer) Register(topic string) (err error) {
	c.consumer[1] = &consumer{
		c:             c,
		getConn:       make(chan []byte,1024),
		heartbeatConn: make(map[string]conns,10),
		lock:          &sync.RWMutex{},
		stopConn:      make(chan struct{}),
	}
	if err := c.consumer[1].write(topic); err != nil {
		return err
	}
	return nil
}

// consume data use need for
func (c *Consumer) Consume() (data string, err error) {
	consumeData := &types.Product{}
	err = proto.Unmarshal(<-c.consumerMessage, consumeData)
	if err != nil {
		return "", err
	}
	data = consumeData.Message
	return
}

func (c *Consumer) init() {
	logger.Logger.Debug("[Consumer][%v] start success",time.Now())
	wrapper.Wrapper(func() {
		l, err := net.Listen("tcp", c.ConsumerAddr)
		if err != nil {
			c.err = err
			return
		}
		for {
			select {
			case <-c.stopServer:
				return
			default:
			}
			conn, err := l.Accept()
			if err != nil {
				logger.Logger.Error("[Consumer.init][%v] accept err %v",time.Now(),err)
				c.close()
				return
			}
			consumer := &consumer{
				c:        c,
				getConn:  make(chan []byte, 10240),
				conn:     conn,
				heartbeatConn:make(map[string]conns,1024),
				lock:&sync.RWMutex{},
				stopConn: make(chan struct{}),
			}
			c.consumer[0] = consumer
			consumer.dispatcher()
		}
	}, "consumer.init")
	return
}

func (c *Consumer) close() {
	close(c.consumerMessage)
	close(c.stopServer)
}

type consumer struct {
	c             *Consumer
	getConn       chan []byte
	conn           net.Conn
	heartbeatConn  map[string]conns
	lock          *sync.RWMutex
	stopConn      chan struct{}
}

type conns struct {
	conn          net.Conn
}

func (c *consumer) dispatcher() {
	wrapper.Wrapper(c.read, "consumer.read")
}

func (c *consumer) read() {
	sizeData := make([]byte, 2)
	for {
		select {
		case <-c.stopConn:
			return
		default:
		}
		if _, err := io.ReadFull(c.conn, sizeData); err != nil {
			logger.Logger.Error("[consumer.read][%v] conn close err %v",time.Now(),err)
			c.close()
			return
		}
		data := make([]byte, binary.LittleEndian.Uint16(sizeData))
		if _, err := io.ReadFull(c.conn, data); err != nil {
			logger.Logger.Error("[consumer.read][%v] conn close err %v",time.Now(),err)
			c.close()
			return
		}
		message := &types.Message{}
		err := proto.Unmarshal(data,message)
		if err != nil {
			logger.Logger.Error("[consumer.read][%v] unmarshal data %v err %v",time.Now(),string(data),err)
			return
		}
		c.c.consumerMessage <- message.Data
	}
}

func (c *consumer) write(topic string) (err error) {
	for k,_ := range c.c.AspireAddr {
		conns := conns{}
		c.lock.RLock()
		if _,ok := c.heartbeatConn[c.c.AspireAddr[k]];!ok {
			c.heartbeatConn[c.c.AspireAddr[k]] = conns
			if conns.conn, err = net.Dial("tcp", c.c.AspireAddr[k]); err != nil {
				return err
			}
			wrapper.Wrapper(c.heartbeat, "consumer.heartbeat")
			message := &types.Message{
				Type:  types.MESSAGECONSUMER,
				Topic: topic,
				Data:  []byte(c.c.ConsumerAddr),
			}
			d, err := proto.Marshal(message)
			if err != nil {
				return err
			}
			data := make([]byte, 2)
			binary.LittleEndian.PutUint16(data, uint16(len(d)))
			data = append(data, d...)
			if _, err := conns.conn.Write(data); err != nil {
				logger.Logger.Error("[consumer.write][%v] write data %v err %v",time.Now(),string(data),err)
				delete(c.heartbeatConn, c.c.AspireAddr[k])
				c.close()
				return err
			}
		}
	}

	return nil
}

// if heartbeat failed delete c.heartbeat addr map
func (c *consumer) heartbeat() {

}

func (c *consumer) close() {
	once.Do(
		func() {
			close(c.stopConn)
			c.c.consumer[0].close()
			c.c.consumer[1].close()
		})
}

// stop consumer
func (c *Consumer) Stop() {
	c.consumer[0].close()
}
