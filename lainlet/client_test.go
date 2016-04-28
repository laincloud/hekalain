package lainlet

import (
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/net/context"
)

var (
	c *Client
)

// go test example:
// `LAINLET_ADDR=192.168.77.21:9001 go test -v`
func init() {
	addr := os.Getenv("LAINLET_ADDR")
	c = NewClient(addr)
}

func TestGet(t *testing.T) {
	if data, err := c.Get("/v2/containers?nodename=node1"); err != nil {
		t.Error(err)
	} else {
		t.Log(string(data))
	}
}

func TestWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	resp, err := c.Watch("/v2/containers?nodename=node1", ctx)
	if err != nil {
		t.Error(err)
	}
	for item := range resp {
		fmt.Println("Id: ", item.Id)
		fmt.Println("Event: ", item.Event)
		fmt.Println("Data: ", string(item.Data))
		fmt.Println("=========================")
	}
}
