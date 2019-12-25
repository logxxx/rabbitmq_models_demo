package manager

import (
	"fmt"
	"github.com/assembla/cony"
	"github.com/streadway/amqp"
	"log"
	"net/http"
)

var (
	g_roterKey2PublisherMap = make(map[string]*cony.Publisher, 0)
)

func InitPublisher() {
	//提供接口:注册成为发布者
	//http://127.0.0.1:8006/regist_publisher?name=xxx
	http.HandleFunc("/regist_publisher", func (w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		routerKey := query.Get("routerkey")

		NewPublisher(routerKey)

		fmt.Fprintf(w, "OK.RouterKey[%v]", routerKey)
		log.Println(fmt.Sprintf("event regist_publisher:routerKey=%v", routerKey))
	})

	//提供接口:发布消息
	//http://127.0.0.1:8006/do_publish?name=xxx&data=xxx
	http.HandleFunc("/do_publish", func (w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		routerKey := query.Get("routerkey")
		data := query.Get("data")
		err := DoPublish(routerKey, data)
		fmt.Fprintf(w, "%v", err)
		log.Println(fmt.Sprintf("event do_publish:routerKey=%v data=%v", routerKey, data))
	})
}


func NewPublisher(routerKey string) {
	pbl := cony.NewPublisher(defaultExchangeName, routerKey)
	g_client.Publish(pbl)

	go func() {
		for {
			select {
			case err := <-g_client.Errors():
				log.Println("publisher find Client err:", err)
			}
		}
	}()

	g_roterKey2PublisherMap[routerKey] = pbl
}

func DoPublish(routerKey string, data string) string {
	publisher, ok := g_roterKey2PublisherMap[routerKey]
	if !ok {
		return "error:not found"
	}

	err := publisher.Publish(amqp.Publishing{
		Body: []byte(data),
	})
	if err != nil {
		return err.Error()
	}
	return "ok"
}