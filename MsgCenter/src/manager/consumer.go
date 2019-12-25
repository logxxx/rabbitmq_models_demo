package manager

import (
	"fmt"
	"github.com/assembla/cony"
	"io/ioutil"
	"log"
	"net/http"
)

func InitConsumer() {
	//提供接口:注册成为消费者
	//http://127.0.0.1:8006/regist_consumer?dest=xxx&callback=xxx
	http.HandleFunc("/regist_consumer", func (w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		routerKey := query.Get("routerkey")
		callbackURL := query.Get("callback")

		newConsumer(routerKey, callbackURL)

		fmt.Fprintf(w, "OK.RouterKey[%v] Callback[%v]",
			routerKey, callbackURL)
		log.Println(fmt.Sprintf("event regist_consumer:routerKey=%v callback=%v", routerKey, callbackURL))
	})
}

func newConsumer(routerKey string, CallbackURL string) {
	queue := &cony.Queue{} //queuename如果不指定，则由mq系统用随机字符串命名

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
		cony.AutoAck(),
	)
	g_client.Consume(consumer)

	go func() {
		for {
			select {
			case msg := <-consumer.Deliveries():
				log.Println(fmt.Sprintf("consumer_recv_msg:%v callback:%v",
					string(msg.Body), CallbackURL))
				go func() {
					HandlerConsumeEvent(CallbackURL, msg.Body)
				}()
			case err := <-consumer.Errors():
				log.Println(fmt.Sprintf("Consumer error: %v", err))
			case err := <-g_client.Errors():
				log.Println(fmt.Sprintf("Client error: %v", err))
			}
		}
	}()
}

func HandlerConsumeEvent(CallbackURL string, msg []byte) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%v?data=%v", CallbackURL, string(msg)), nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(fmt.Sprintf("HandlerConsumeEvent-client.Do fail.err:%v",err))
	}
	defer resp.Body.Close()
	s,err:=ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(fmt.Sprintf("HandlerConsumeEvent-ioutil.ReadAll fail.err:%v",err))
	}
	log.Println("HandlerConsumeEvent.resp:", string(s))

}