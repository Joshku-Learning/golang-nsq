package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"sync"
)

type myMessageHandler struct {
	Title string
}

func (h *myMessageHandler) HandleMessage(m *nsq.Message) (err error) {
	log.Fatalf("receive a message: %v", m)
	fmt.Printf("%s receive from %v\n", h.Title, m.NSQDAddress, string(m.Body))
	log.Fatalf("this message convert to string is:%v", string(m.Body))
	return
}

func main() {
	wg := &sync.WaitGroup{}
	wg.Add(1000)
	config := nsq.NewConfig()
	c, _ := nsq.NewConsumer("testTopic", "ch", config)
	c.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		log.Printf("Got a message:%s", message.Body)
		wg.Done()
		return nil
	}))
	// 直接連nsqs
	err := c.ConnectToNSQD("127.0.0.1:1027")
	if err != nil {
		log.Panic(err)
	}
	//// 通過nsqlookup 服務發現
	//err := c.ConnectToNSQLookupd("127.0.0.1:1024")
	//if err != nil {
	//	log.Panic(err)
	//}
	wg.Wait()


}
