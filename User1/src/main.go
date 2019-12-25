package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
)

var (
	mailServiceURL = "http://127.0.0.1:8006"
	consumerURL    = "http://127.0.0.1:8001"
)

func doRegistConsumer(consumerName string, routerKey string, callback string) {
	params := fmt.Sprintf("?name=%v&routerkey=%v&callback=%v", consumerName, routerKey, callback)
	finalUrl := mailServiceURL + "/regist_consumer" + params
	log.Println("req:", finalUrl)
	req, err := http.NewRequest("GET", finalUrl, nil)
	if err != nil {
		log.Fatal("get err:%v", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(fmt.Sprintf("register_consumer fail.err:%v", err))
	}
	s, err := ioutil.ReadAll(resp.Body)
	log.Println("resp:", string(s))
}

func doUnRegistConsumer(consumerName string) {
	params := fmt.Sprintf("?name=%v", consumerName)
	finalUrl := mailServiceURL + "/unregist_consumer" + params
	log.Println("req:", finalUrl)
	req, err := http.NewRequest("GET", finalUrl, nil)
	if err != nil {
		log.Println("get err:%v", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(fmt.Sprintf("unregister_consumer fail.err:%v", err))
	}
	s, err := ioutil.ReadAll(resp.Body)
	log.Println("resp:", string(s))
}

func main() {
	routerKey := "order"
	callback := consumerURL + "/callback"
	consumerName := "User1"

	log.Println("=====1.注册成为消费者")
	doRegistConsumer(consumerName, routerKey, callback)

	log.Println("=====2.监听回调")
	http.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		log.Println(consumerName, "recv data:", query.Get("data"))
		fmt.Fprintf(w, "ok")
	})

	log.Println("listen and reserve", consumerURL, "...")
	go http.ListenAndServe(":8001", nil)

	//捕获程序退出
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
	log.Println("=====3.取消注册并结束程序")
	doUnRegistConsumer(consumerName)
}
