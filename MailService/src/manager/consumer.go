package manager

import (
	"fmt"
	"github.com/assembla/cony"
	"io/ioutil"
	"log"
	"net/http"
)

var (
	defaultExchangeName = "default_exchange"
	defalutExchangeKind = "direct"
	stop                = make(chan struct{})
)

func InitConsumer() {
	//提供接口:注册成为消费者
	//http://127.0.0.1:8006/regist_consumer?name=xxx&routerkey=xxx&callback=xxx
	http.HandleFunc("/regist_consumer", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		name := query.Get("name")
		routerKey := query.Get("routerkey")
		callbackURL := query.Get("callback")

		newConsumer(name, routerKey, callbackURL)

		fmt.Fprintf(w, "Regist Succ.Name[%v] RouterKey[%v] Callback[%v]",
			name, routerKey, callbackURL)
		log.Println(fmt.Sprintf("event regist_consumer: name=%v routerKey=%v callback=%v",
			name, routerKey, callbackURL))
	})
}

func newConsumer(name string, routerKey string, CallbackURL string) {
	queue := &cony.Queue{
		Name: "q_" + routerKey,
	}

	exchange := cony.Exchange{
		Name: defaultExchangeName,
		Kind: defalutExchangeKind,
	}

	bind := cony.Binding{
		Queue:    queue,
		Exchange: exchange,
		Key:      routerKey,
	}

	g_client.Declare([]cony.Declaration{
		cony.DeclareQueue(queue),
		cony.DeclareExchange(exchange),
		cony.DeclareBinding(bind),
	})

	consumer := cony.NewConsumer(
		queue,
	)
	g_client.Consume(consumer)

	g_consumers[name] = consumer

	go func() {
		for g_client.Loop() {
			select {
			case <-stop:
				log.Println(fmt.Sprintf("One Consumer unregisted."))
				return
			case err := <-consumer.Errors():
				log.Println(fmt.Sprintf("Consumer error: %v", err))
			case err := <-g_client.Errors():
				log.Println(fmt.Sprintf("Client error: %v", err))
			case msg := <-consumer.Deliveries():
				go func() {
					ok := HandleConsumeEvent(CallbackURL, msg.Body)
					if ok {
						msg.Ack(true)
					} else {
						msg.Nack(true, true)
					}
				}()
			}
		}
	}()
}

func HandleConsumeEvent(CallbackURL string, msg []byte) bool {
	log.Println(">>>>>>>>EVENT consumer recv msg")
	log.Println(fmt.Sprintf("msg:%v callback:%v",
		string(msg), CallbackURL))
	req, _ := http.NewRequest("GET", fmt.Sprintf("%v?data=%v", CallbackURL, string(msg)), nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(fmt.Sprintf("HandleConsumeEvent-client.Do fail.err:%v", err))
		return false
	}

	defer resp.Body.Close()
	s, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(fmt.Sprintf("HandleConsumeEvent-ioutil.ReadAll fail.err:%v", err))
		return false
	}
	log.Println("callback.resp:", string(s))
	if string(s) != "ok" {
		return false
	}
	return true
}
