package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var (
	mqURL     = "http://127.0.0.1:8006"
	routerKey = "order"
)

func main() {
	//1.注册成为发布者
	params := fmt.Sprintf("?routerkey=%v", routerKey)
	log.Println("params:%v", params)
	req, _ := http.NewRequest("GET", mqURL+"/regist_publisher"+params, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(fmt.Sprintf("register_publisher fail.err:%v",err))
	}
	s,err:=ioutil.ReadAll(resp.Body)
	log.Println("register.resp:", string(s))

	//2.等待输入消息并推送给mq
	input := bufio.NewScanner(os.Stdin)
	fmt.Printf("Please type in something to publish:\n")

	for input.Scan() {
		//推送给mq
		params := fmt.Sprintf("?routerkey=%v&data=%v", routerKey, input.Text())
		req, _ := http.NewRequest("GET", mqURL+"/do_publish"+params, nil)
		resp, err := client.Do(req)
		if err != nil {
			panic(fmt.Sprintf("do_publish fail.err:%v",err))
		}
		s,err:=ioutil.ReadAll(resp.Body)
		log.Println(string(s))
	}
}