package common

import (
	"AspireMQ/api/types"
	"AspireMQ/staging/src/aspire.mq/id"
	"AspireMQ/staging/src/safe_map"
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
