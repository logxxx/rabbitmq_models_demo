package main

//docker run -d --hostname localhost --name myrabbit -p 15672:15672 -p 5672:5672 rabbitmq

import (
	"log"
	"manager"
	"net/http"
)

func main() {
	manager.InitClient()
	manager.InitConsumer()
	manager.InitPublisher()
	log.Println("init succ. start listen and serve...")
	http.ListenAndServe(":8006", nil)
}