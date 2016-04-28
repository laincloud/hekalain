package webrouter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/laincloud/hekalain/pkgs/hekahelper"
)

const nginxLogTimeFormat = "2/Jan/2006:15:04:05 -0700"

type KafkaLogItem struct {
	Hostname string `json:"hostname"`
	Message  string `json:"message"`
}

type NginxLogItem struct {
	RemoteAddr           string `json:"remote_addr"`
	RemoteUser           string `json:"remote_user"`
	TimeLocal            string `json:"time_local"`
	Host                 string `json:"host"`
	Request              string `json:"request"`
	Status               int    `json:"status"`
	BodyBytesSent        int    `json:"body_bytes_sent"`
	HTTPReferer          string `json:"http_referer"`
	HTTPUserAgent        string `json:"http_user_agent"`
	HTTPXForwardedFor    string `json:"http_x_forwarded_for"`
	UpstreamResponseTime string `json:"upstream_response_time"`
	RequestTime          string `json:"request_time"`
}

type KafkaEncoder struct {
	reportLock             sync.RWMutex
	processDuration        int64
	processSamples         int64
	processMessageCount    int64
	processMessageDiscards int64
	processMessageFailures int64
}

func (enc *KafkaEncoder) Init(config interface{}) error {
	return nil
}

func (enc *KafkaEncoder) Encode(pack *pipeline.PipelinePack) ([]byte, error) {
	start := time.Now()
	message := pack.Message

	var logItem NginxLogItem
	logItem.RemoteAddr, _ = hekahelper.GetMessageStringValue(message, "remote_addr")
	logItem.RemoteUser, _ = hekahelper.GetMessageStringValue(message, "remote_user")
	logItem.TimeLocal = time.Unix(0, message.GetTimestamp()).Format(nginxLogTimeFormat)
	logItem.Host, _ = hekahelper.GetMessageStringValue(message, "Host")
	logItem.Request, _ = hekahelper.GetMessageStringValue(message, "request")
	status, _ := hekahelper.GetMessageDoubleValue(message, "status")
	logItem.Status = int(status)
	bodyBytesSent, _ := hekahelper.GetMessageDoubleValue(message, "body_bytes_sent")
	logItem.BodyBytesSent = int(bodyBytesSent)
	logItem.HTTPReferer, _ = hekahelper.GetMessageStringValue(message, "http_referer")
	logItem.HTTPUserAgent, _ = hekahelper.GetMessageStringValue(message, "http_user_agent")
	logItem.HTTPXForwardedFor, _ = hekahelper.GetMessageStringValue(message, "http_x_forwarded_for")
	urt, _ := hekahelper.GetMessageDoubleValue(message, "upstream_response_time")
	rt, _ := hekahelper.GetMessageDoubleValue(message, "request_time")
	logItem.UpstreamResponseTime = strconv.FormatFloat(urt, 'f', 3, 64)
	logItem.RequestTime = strconv.FormatFloat(rt, 'f', 3, 64)

	if logItem.Status == 0 {
		atomic.AddInt64(&enc.processMessageDiscards, 1)
		return nil, fmt.Errorf("Found log item with empty http status code")
	}
	data, err := json.Marshal(logItem)
	if err == nil {
		kafkaLog := KafkaLogItem{
			Message:  string(data),
			Hostname: message.GetHostname(),
		}
		if data, err = json.Marshal(kafkaLog); err == nil {
			atomic.AddInt64(&enc.processMessageCount, 1)
			enc.reportLock.Lock()
			enc.processDuration += time.Since(start).Nanoseconds()
			enc.processSamples++
			enc.reportLock.Unlock()
		}
	}
	if err != nil {
		atomic.AddInt64(&enc.processMessageFailures, 1)
	}
	return data, err
}

func (enc *KafkaEncoder) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&enc.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures", atomic.LoadInt64(&enc.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageDiscards", atomic.LoadInt64(&enc.processMessageDiscards), "count")
	var avgDuration int64
	enc.reportLock.RLock()
	if enc.processSamples > 0 {
		avgDuration = enc.processDuration / enc.processSamples
	}
	enc.reportLock.RUnlock()
	message.NewInt64Field(msg, "ProcessMessageAvgDuration", avgDuration, "ns")
	return nil
}

func init() {
	pipeline.RegisterPlugin("WebRouterKafkaEncoder", func() interface{} {
		return new(KafkaEncoder)
	})
}
