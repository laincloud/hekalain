package main

import (
	"flag"
	"fmt"
	"net"
	"sync"
	"time"
)

func main() {
	var containerId string
	flag.StringVar(&containerId, "cid", "", "")
	flag.Parse()

	if containerId == "" {
		panic("please provide container id")
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", "127.0.0.1:8178")
			if err != nil {
				panic(err)
			}

			for j := 0; j < 10000; j += 1 {
				fmt.Fprintf(conn, mkMessage(containerId))
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

func mkMessage(cId string) string {
	now := time.Now()
	return fmt.Sprintf("<30> %s n1 docker/%s[965]: From load test %s\n", now.Format(time.RFC3339Nano), cId, now.Format(time.Kitchen))
}
