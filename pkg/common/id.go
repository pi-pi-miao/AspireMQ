package common

import (
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/id"
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
)

var (
	MessageId       chan string       // id
	MessageIdStop   chan bool         // controller id chan
	SendMessage     chan *types.Messages       // all product send message to this channel
	SendMessageFlag bool              // sendMessage flag
	TemporaryCache  *safe_map.SyncMap // this is temporary map
)

func SetId() {
	for {
		select {
		case <-MessageIdStop:
			close(MessageId)
			return
		default:
		}
		MessageId <- id.GetId()
	}
}
