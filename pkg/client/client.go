package client

import (
	"github.com/pi-pi-miao/AspireMQ/api/types"
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
	AspireMQAddr []string
	Conn         []net.Conn
	GetData      chan []byte
}

func ReportOurSelft(ourAddr string, originAddr []string) error {
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
	for k,_ := range cli.AspireMQAddr {
		conn,err := net.Dial("tcp",cli.AspireMQAddr[k])
		if err != nil {
			return nil, err
		}
		c.Conn = append(c.Conn,conn)
	}
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
	for k,_ := range c.Conn {
		if _, err := c.Conn[k].Write(data); err != nil {
			c.Conn[k].Close()
			c.Conn = append(c.Conn[:k],c.Conn[k+1:]...)
			return nil, err
		}
	}
	return c, nil
}

// todo cpu,disk,mem,fileInfo,heartbeat,something...
func (c *Client)ReportOurHealth(){

}