package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

var (
	mailServiceURL = "http://127.0.0.1:8006"
	consumerURL    = "http://127.0.0.1:8003"
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

func main() {
	routerKey := "order"
	callback := consumerURL + "/callback"
	consumerName := "User3"

	log.Println("=====1.注册成为消费者")
	doRegistConsumer(consumerName, routerKey, callback)

	log.Println("=====2.监听回调")
	http.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		log.Println(consumerName, "recv data:", query.Get("data"))
		fmt.Fprintf(w, "ok")
	})

	log.Println("listen and reserve", consumerURL, "...")
	http.ListenAndServe(":8003", nil)
}
