package manager

import (
	"fmt"
	"github.com/assembla/cony"
)

var (
	g_client *cony.Client
)

func InitClient() {
	connAddr := fmt.Sprintf("amqp://%s:%s@%s/",
		"guest", "guest", "127.0.0.1:5672")

	g_client = cony.NewClient(cony.URL(connAddr))

	g_client.Loop()
}
