package types

type Messages struct {
	Type string
	Data []byte
	Group string
}


func NewMessages()*Messages{
	return &Messages{
		Data:make([]byte,0,1024),
	}
}