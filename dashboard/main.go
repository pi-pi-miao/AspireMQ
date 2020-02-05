package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/pi-pi-miao/AspireMQ/dashboard/authentication"
	"github.com/pi-pi-miao/AspireMQ/dashboard/getData"
	"github.com/pi-pi-miao/AspireMQ/pkg/logger"
	"github.com/pi-pi-miao/AspireMQ/staging/src/safe_map"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

var (
	upgrader = websocket.Upgrader {
		// 读取存储空间大小
		ReadBufferSize:1024,
		// 写入存储空间大小
		WriteBufferSize:1024,
		// 允许跨域
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)


func AspireMQ(w http.ResponseWriter, r *http.Request){
	switch r.Method {
	case http.MethodGet:
		GetAspireMQ(w,r)
	case http.MethodPost:
		SetAspireMq(w,r)
	case http.MethodDelete:
		DelAspireMq(w,r)
	}
}

func GetAspireMQ(w http.ResponseWriter,r *http.Request){
	mq := &getData.AspireMQ{}
	getData.Mq.Addrs.EachItem(func(item *safe_map.Item) {
		mq.Addr = append(mq.Addr,item.Key)
	})
	io.WriteString(w,fmt.Sprintf("%v",mq.Addr))
	return
}

func SetAspireMq(w http.ResponseWriter, r *http.Request){
	mq := &getData.AspireMQ{}
	body,err := ioutil.ReadAll(r.Body)
	if err != nil {
		io.WriteString(w,"input body err")
		return
	}
	err = json.Unmarshal(body,mq)
	if err != nil {
		io.WriteString(w,fmt.Sprintf("%v",err))
		return
	}
	if err := getData.SetData(mq.Addr);err != nil {
		io.WriteString(w,fmt.Sprintf("%v",err))
		return
	}
	io.WriteString(w,"set success")
	return
}

func DelAspireMq(w http.ResponseWriter, r *http.Request) {
	mq := &getData.AspireMQ{}
	body,err := ioutil.ReadAll(r.Body)
	if err != nil {
		io.WriteString(w,"input body err")
		return
	}
	err = json.Unmarshal(body,mq)
	if err != nil {
		io.WriteString(w,fmt.Sprintf("%v",err))
		return
	}
	getData.DelData(mq.Addr)
	io.WriteString(w,"delete success")
	return
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		wbsCon *websocket.Conn
		err error
	)
	if wbsCon, err = upgrader.Upgrade(w, r, nil);err != nil {
		return
	}

	for data := range getData.Message{
		if err = wbsCon.WriteMessage(websocket.TextMessage, data); err != nil {
			goto ERR
		}
	}
ERR:
	wbsCon.Close()
}

func main()  {
	getData.NewAspireMQ()
	logger.New("","debug","./src/github.com/pi-pi-miao/AspireMQ/log/",0)
	http.HandleFunc("/login",authentication.Login)
	http.HandleFunc("/input_aspire",AspireMQ)
	http.HandleFunc("/ws",wsHandler)
	//http.Handle("/", http.FileServer(http.Dir("./src/github.com/pi-pi-miao/AspireMQ/dashboard")))
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe", err.Error())
	}
}