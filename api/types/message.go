package types

type Messages struct {
	Type string
	Data []byte
}


func NewMessages(t string,data []byte)*Messages{
	return &Messages{
		Type:t,
		Data:data,
	}
}