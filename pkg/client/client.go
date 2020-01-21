package client

import (
	"AspireMQ/api/types"
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"net"
	"runtime"
)

var (
	cli *Client
)

type Client struct {
	OurAddr      string
	AspireMQAddr string
	Conn         net.Conn
	GetData      chan []byte
}

func ReportOurSelft(ourAddr, originAddr string) error {
	cli = &Client{}
	cli.OurAddr = ourAddr
	cli.AspireMQAddr = originAddr
	_, err := cli.Register()
	if err != nil {
		return err
	}
	_, err = cli.ReportOurCpuNum()
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Register() (*Client, error) {
	conn, err := net.Dial("tcp", cli.AspireMQAddr)
	if err != nil {
		return nil, err
	}
	c.Conn = conn
	return c, nil
}

func (c *Client) ReportOurCpuNum() (*Client, error) {
	o, err := proto.Marshal(&types.OurSelf{
		Addr:   c.OurAddr,
		CpuNum: int32(runtime.NumCPU() - 1),
	})
	if err != nil {
		// todo 打印日志
	}
	ourInfo, err := proto.Marshal(&types.Message{
		Type: types.MESSAGEOURSELFTYPE,
		Data: o,
	})
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, uint16(len(ourInfo)))
	data = append(data, ourInfo...)
	if _, err := c.Conn.Write(data); err != nil {
		c.Conn.Close()
		return nil, err
	}
	return c, nil
}
