package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

var (
	msgCenterURL = "http://127.0.0.1:8006"
	consumerURL  = "http://127.0.0.1:8002"
)

func doRegistConsumer(routerKey string, callback string) {
	log.Println("=====1.注册成为消费者")
	params := fmt.Sprintf("?routerkey=%v&callback=%v", routerKey, callback)
	finalUrl := msgCenterURL +"/regist_consumer"+params
	log.Println("req:", finalUrl)
	req, err := http.NewRequest("GET", finalUrl, nil)
	if err != nil {
		log.Fatal("get err:%v", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(fmt.Sprintf("register_consumer fail.err:%v",err))
	}
	s,err:=ioutil.ReadAll(resp.Body)
	log.Println("resp:", string(s))
}

func waitForCallback() {
	log.Println("=====2.监听回调")
	http.HandleFunc("/callback", func (w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		log.Println("consumer2 recv data:", query.Get("data"))
		fmt.Fprintf(w, "data received by consumer2.")
	})
}

func main() {
	routerKey := "order"
	callback := consumerURL+"/callback"
	doRegistConsumer(routerKey, callback)

	waitForCallback()

	log.Println("listen and reserve", consumerURL, "...")
	http.ListenAndServe(":8002", nil)
}