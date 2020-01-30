package common

import (
	"github.com/pi-pi-miao/AspireMQ/api/types"
	"github.com/pi-pi-miao/AspireMQ/staging/src/aspire.mq/wrapper"
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
)

func InitCommon(){
	SendMessageFlag = true
	MessageId = make(chan string,1024)
	MessageIdStop = make(chan bool)
	SendMessage  = make(chan *types.Messages,2000)
	TemporaryCache = safe_map.New()
	wrapper.Wrapper(SetId,"get_id.[SetId]")
}
