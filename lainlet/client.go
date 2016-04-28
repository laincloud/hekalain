package lainlet

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/net/context"
)

type Response struct {
	Id       int64
	Event    string
	Data     []byte
	finished bool
}

type Client struct {
	address    string
	httpClient *http.Client
}

func (c *Client) Get(uri string) ([]byte, error) {
	reader, err := c.doRequest(uri, false)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)

}

func (c *Client) Watch(uri string, ctx context.Context) (<-chan *Response, error) {
	reader, err := c.doRequest(uri, true)
	if err != nil {
		return nil, err
	}
	respCh := make(chan *Response)
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-stop:
		}
		reader.Close()
	}()
	go func() {
		defer close(stop)
		defer close(respCh)
		var (
			newone bool = true // if need to create a new response
			resp   *Response
		)
		buf := bufio.NewReader(reader)
		for {
			if newone {
				resp = new(Response)
				resp.finished = false
				newone = false
			}
			line, err := buf.ReadBytes('\n')
			if err != nil {
				if err != io.EOF || len(line) == 0 {
					return
				}
			}
			if pureLine := bytes.TrimSpace(line); len(pureLine) == 0 {
				continue
			} else {
				fields := bytes.SplitN(pureLine, []byte{':'}, 2)
				if len(fields) < 2 {
					continue
				}
				switch key := string(bytes.TrimSpace(fields[0])); key {
				case "id":
					if id, err := strconv.ParseInt(string(bytes.TrimSpace(fields[1])), 10, 64); err == nil {
						resp.Id = id
					}
				case "event":
					resp.Event = string(bytes.TrimSpace(fields[1]))
					if resp.Event == "heartbeat" {
						resp.finished = true
					}
				case "data":
					resp.Data = bytes.TrimSpace(fields[1])
					resp.finished = true
				}
			}
			if resp.finished {
				respCh <- resp
				newone = true
			}
		}
	}()
	return respCh, nil
}

func (c *Client) doRequest(uri string, watch bool) (io.ReadCloser, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	u.Scheme = "http"
	u.Host = c.address

	q := u.Query()
	if watch {
		q.Set("watch", "1")
	} else {
		q.Set("watch", "0")
	}
	u.RawQuery = q.Encode()

	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func NewClient(address string, to ...time.Duration) *Client {
	client := &Client{
		address: address,
	}

	timeout := time.Duration(0)
	if len(to) > 0 {
		timeout = to[0]
	}
	client.httpClient = &http.Client{
		Timeout: timeout,
	}
	return client
}
