package syslog

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/laincloud/hekalain/pkgs/hekahelper"
)

type ProcLogItem struct {
	Priority    int       `json:"priority"`
	Hostname    string    `json:"hostname"`
	Message     string    `json:"message"`
	Pid         int       `json:"pid"`
	Timestamp   time.Time `json:"timestamp"`
	ContainerId string    `json:"container_id"`
	LogFile     string    `json:"log_file"`
	ProcName    string    `json:"proc_name"`
	ProcAppName string    `json:"app_name"`
	ContainerIP string    `json:"container_ip"`
	InstanceNo  int       `json:"instance_no"`
}

type SyslogLainEncoder struct {
	reportLock             sync.RWMutex
	processDuration        int64
	processSamples         int64
	processMessageCount    int64
	processMessageDiscards int64
	processMessageFailures int64
}

func (enc *SyslogLainEncoder) Init(config interface{}) error {
	return nil
}

func (enc *SyslogLainEncoder) Encode(pack *pipeline.PipelinePack) ([]byte, error) {
	start := time.Now()
	message := pack.Message

	var logItem ProcLogItem
	logItem.Priority = int(message.GetSeverity())
	logItem.Hostname = message.GetHostname()
	logItem.Message = message.GetPayload()
	logItem.Pid = int(message.GetPid())
	logItem.Timestamp = time.Unix(0, message.GetTimestamp()).UTC()
	logItem.ContainerId, _ = hekahelper.GetMessageStringValue(message, "container_id")
	logItem.LogFile, _ = hekahelper.GetMessageStringValue(message, "log_file")
	logItem.ProcName, _ = hekahelper.GetMessageStringValue(message, "lain_proc_name")
	logItem.ProcAppName, _ = hekahelper.GetMessageStringValue(message, "lain_app_name")
	logItem.ContainerIP, _ = hekahelper.GetMessageStringValue(message, "lain_container_ip")
	logItem.InstanceNo, _ = hekahelper.GetMessageIntValue(message, "lain_instance_no")

	if logItem.ProcName == "" || logItem.ProcAppName == "" {
		atomic.AddInt64(&enc.processMessageDiscards, 1)
		return nil, fmt.Errorf("Found log item with empty app name or proc name")
	}
	data, err := json.Marshal(logItem)
	if err == nil {
		atomic.AddInt64(&enc.processMessageCount, 1)
		enc.reportLock.Lock()
		enc.processDuration += time.Since(start).Nanoseconds()
		enc.processSamples++
		enc.reportLock.Unlock()
	} else {
		atomic.AddInt64(&enc.processMessageFailures, 1)
	}
	return data, err
}

func (enc *SyslogLainEncoder) ReportMsg(msg *message.Message) error {
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
	pipeline.RegisterPlugin("SyslogLainEncoder", func() interface{} {
		return new(SyslogLainEncoder)
	})
}
