# Welcome to AspireMQ

```
AspireMQ is a high performance
```

## direction for use

### product

```go
package main

import (
	"fmt"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire_mq"
	"time"
)

// aspireMQ server Addr
var originAddr = []string{
	"127.0.0.1:8081",
}

func main(){
	aspireMQ,err := aspire_mq.Aspire("127.0.0.1","8080",originAddr)
	if err != nil {
		fmt.Println("conn aspire err",err)
		return
	}
	aspireMQ.Publish("aspire","hello")
	time.Sleep(3*time.Second)
}

```



AspireMQ server

```go
go build github.com/pi-pi-miao/AspireMQ/cmd/aspired
aspired start --addr=127.0.0.1:8081
```



### consumer

```go
package main

import (
	"fmt"
	"github.com/pi-pi-miao/AspireMQ/pkg/aspire_consumer"
)

var (
	aspireAddr = []string{
		"127.0.0.1:8081",
	}
	cusumerAddr = "127.0.0.1:8082"
)

func main(){
	consumer := aspire_consumer.NewConsumer(cusumerAddr,aspireAddr)
	consumer.AspireConsumer()
	consumer.Register("aspire")

	data,err := consumer.Consume()
	if err != nil {
		fmt.Println("consumer failed",err)
		return
	}
	fmt.Println("consume data is ",data)
}

```

 WARNING: 

```
AspireMQ is in beta, please do not use it for production clusters
```

