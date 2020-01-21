package id

import (
	"fmt"
	uuid "github.com/satori/go.uuid"
)

func GetId() string {
	id, err := uuid.NewV4()
	if err != nil {
		// todo ...
	}
	return fmt.Sprintf("%v", id)
}
