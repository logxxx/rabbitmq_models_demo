package manager

import (
	"fmt"
	"github.com/assembla/cony"
	"io/ioutil"
	"log"
	"net/http"
)

var (
	isQueueUniq         = true
	defaultExchangeName = "default_exchange"
	defalutExchangeKind = "direct"
	stop                = make(chan struct{})
)

func InitConsumer() {
	//提供接口:注册成为消费者
	//http://127.0.0.1:8006/regist_consumer?dest=xxx&callback=xxx
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

	//提供接口:取消注册
	//http://127.0.0.1:8006/unregist_consumer?name=xxx
	http.HandleFunc("/unregist_consumer", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		name := query.Get("name")

		c, ok := consumers[name]
		if ok {
			stop <- struct{}{}
			c.Cancel()
			consumers[name] = nil
		}

		fmt.Fprintf(w, "UnRegist Succ.")
		log.Println(fmt.Sprintf("event unregist_consumer: name=%v", name))
	})
}

func newConsumer(name string, routerKey string, CallbackURL string) {
	queue := &cony.Queue{}

	queueName := "q_" + routerKey
	if isQueueUniq {
		queueName = "" //queuename如果不指定，则由mq系统用随机字符串命名
	}
	queue.Name = queueName

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
		cony.Tag(name),
	)
	g_client.Consume(consumer)

	consumers[name] = consumer

	go func() {
		for {
			select {
			case <-stop:
				log.Println(fmt.Sprintf("One Consumer unregisted."))
				return
			case err := <-consumer.Errors():
				log.Println(fmt.Sprintf("Consumer error: %v", err))
			case err := <-g_client.Errors():
				log.Println(fmt.Sprintf("Client error: %v", err))
			case msg := <-consumer.Deliveries():
				log.Println(fmt.Sprintf("xxxxxxxxmsg:%#v"), msg)
				if len(msg.Body) == 0 {
					continue
				}
				go func() {
					ok := HandleConsumeEvent(consumer, CallbackURL, msg.Body)
					if !ok {
						msg.Nack(false, true)
					} else {
						msg.Ack(false)
					}
				}()
			}
		}
	}()
}

func HandleConsumeEvent(consumer *cony.Consumer, CallbackURL string, msg []byte) bool {
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
	return true
}
