package types

type Messages struct {
	Type string
	Data []byte
	Topic string
}


func NewMessages(t,topic string,data []byte)*Messages{
	return &Messages{
		Type:t,
		Data:data,
		Topic:topic,
	}
}