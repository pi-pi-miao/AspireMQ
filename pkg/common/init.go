package common

import (
	"AspireMQ/api/types"
	"AspireMQ/staging/src/aspire.mq/wrapper"
	"AspireMQ/staging/src/safe_map"
)

func InitCommon(){
	SendMessageFlag = true
	MessageId = make(chan string,1000)
	MessageIdStop = make(chan bool)
	SendMessage  = make(chan *types.Messages,2000)
	TemporaryCache = safe_map.New()
	wrapper.Wrapper(SetId,"get_id.[SetId]")
}
