package aspire_consumer

import (
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"io"
	"net"
)

var (
	ConsumerMessage = make(chan []byte, 10240)
)

type Consumer struct {
	ConsumerAddr string
	AspireAddr   string
	err          error
	stopServer   chan struct{}
	consumer     [1]*consumer
}

func NewConsumer() *Consumer {
	return &Consumer{
		stopServer: make(chan struct{}),
		consumer:   [1]*consumer{},
	}
}

// consumer start
func (c *Consumer) AspireConsumer() error {
	if c.AspireAddr == "" && c.ConsumerAddr == "" {
		return errors.New("input aspireAddr or consumerAddr is not input")
	}
	c.init()
	if c.err != nil {
		return c.err
	}
	return nil
}

// register topic
func (c *Consumer) Register(topic string) (err error) {
	if err := c.consumer[0].write(topic); err != nil {
		return err
	}
	return nil
}

// consume data use need for
func (c *Consumer) consume() (data string, err error) {
	consumeData := &types.Product{}
	err = proto.Unmarshal(<-ConsumerMessage, consumeData)
	if err != nil {
		return "", err
	}
	data = consumeData.Message
	return
}

func (c *Consumer) init() {
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
				// todo add log
				c.close()
				return
			}
			consumer := &consumer{
				c:        c,
				getConn:  make(chan []byte, 10240),
				conn:     conn,
				stopConn: make(chan struct{}),
			}
			c.consumer[0] = consumer
			consumer.dispatcher()
		}
	}, "consumer.init")
	return
}

func (c *Consumer) close() {
	close(c.stopServer)
}

type consumer struct {
	c             *Consumer
	getConn       chan []byte
	conn          net.Conn
	heartbeatConn net.Conn
	stopConn      chan struct{}
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
			c.close()
			return
		}
		data := make([]byte, binary.LittleEndian.Uint16(sizeData))
		if _, err := io.ReadFull(c.conn, data); err != nil {
			c.close()
			return
		}
		ConsumerMessage <- data
	}
}

func (c *consumer) write(topic string) (err error) {
	if c.heartbeatConn, err = net.Dial("tcp", c.c.AspireAddr); err != nil {
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
	if _, err := c.heartbeatConn.Write(data); err != nil {
		// todo add log
		c.close()
		return err
	}
	return nil
}

func (c *consumer) heartbeat() {

}

func (c *consumer) close() {
	close(c.stopConn)
	c.c.consumer[0].close()
}

// stop consumer
func (c *Consumer) Stop() {
	close(ConsumerMessage)
	c.consumer[0].close()
}
