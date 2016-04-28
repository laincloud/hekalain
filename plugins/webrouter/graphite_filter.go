package webrouter

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
)

type GraphiteFilterConfig struct {
	OutputMsgType string `toml:"output_message_type"`
}

type GraphiteFilter struct {
	fr            pipeline.FilterRunner
	h             pipeline.PluginHelper
	msgType       string
	hostName      string
	cancelMonitor context.CancelFunc

	reportLock             sync.RWMutex
	processDuration        int64
	processSamples         int64
	processMessageCount    int64
	processMessageDiscards int64
}

func (f *GraphiteFilter) ConfigStruct() interface{} {
	conf := &GraphiteFilterConfig{
		OutputMsgType: "graphite.stat",
	}
	return conf
}

func (f *GraphiteFilter) Init(config interface{}) error {
	var err error
	if f.hostName, err = os.Hostname(); err != nil {
		return err
	}
	if conf, ok := config.(*GraphiteFilterConfig); ok {
		_, f.cancelMonitor = context.WithCancel(context.Background())
		f.msgType = conf.OutputMsgType
		return nil
	}
	return fmt.Errorf("Invalid WebRouterGraphiteFilter config object.")
}

func (f *GraphiteFilter) Prepare(fr pipeline.FilterRunner, h pipeline.PluginHelper) error {
	f.fr = fr
	f.h = h
	return nil
}

func (f *GraphiteFilter) CleanUp() {
	f.fr.LogMessage("WebRouterGraphiteFilter got command to clean up")
	f.cancelMonitor()
	_, f.cancelMonitor = context.WithCancel(context.Background())
}

func (f *GraphiteFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	defer func() {
		f.fr.UpdateCursor(pack.QueueCursor)
	}()

	start := time.Now()
	msg := pack.Message

	msgLoopCount := pack.MsgLoopCount
	var newPack *pipeline.PipelinePack
	var err error
	if newPack, err = f.h.PipelinePack(msgLoopCount); err != nil {
		// We should retry to create new pack
		return pipeline.NewRetryMessageError("Cannot pipeline pack, %s", err.Error())
	}

	dataMap := make(map[string]HTTPStatusStat)
	timestamp := msg.GetTimestamp() / 1e9
	if err := json.Unmarshal([]byte(msg.GetPayload()), &dataMap); err != nil {
		atomic.AddInt64(&f.processMessageDiscards, 1)
		return fmt.Errorf("WebRouterGraphiteEncoder marshalling data failed: %s", err.Error())
	}
	lines := make([]string, 0, len(dataMap)*8)
	for domain, stat := range dataMap {
		formattedDomain := strings.Replace(domain, ".", "_", -1)
		lines = append(lines,
			fmt.Sprintf("webrouter.%s.nginx_access.%s.http_500 %d %d", f.hostName, formattedDomain, stat.HTTP500, timestamp),
			fmt.Sprintf("webrouter.%s.nginx_access.%s.http_501 %d %d", f.hostName, formattedDomain, stat.HTTP501, timestamp),
			fmt.Sprintf("webrouter.%s.nginx_access.%s.http_502 %d %d", f.hostName, formattedDomain, stat.HTTP503, timestamp),
			fmt.Sprintf("webrouter.%s.nginx_access.%s.http_503 %d %d", f.hostName, formattedDomain, stat.HTTP503, timestamp),
			fmt.Sprintf("webrouter.%s.nginx_access.%s.http_504 %d %d", f.hostName, formattedDomain, stat.HTTP504, timestamp),
			fmt.Sprintf("webrouter.%s.nginx_access.%s.http_505 %d %d", f.hostName, formattedDomain, stat.HTTP505, timestamp),
			fmt.Sprintf("webrouter.%s.nginx_access.%s.avg_cost_sec %.3f %d", f.hostName, formattedDomain, stat.AvgTimeCost, timestamp),
			fmt.Sprintf("webrouter.%s.nginx_access.%s.qpm %.3f %d", f.hostName, formattedDomain, stat.QueriesPerMin, timestamp),
		)
	}

	newPack.Message = message.CopyMessage(msg)
	newPack.Message.SetLogger(f.fr.Name())
	newPack.Message.SetType(f.msgType)
	newPack.Message.SetUuid(uuid.NewRandom())
	newPack.Message.SetPayload(strings.Join(lines, "\n"))

	f.fr.Inject(newPack)
	atomic.AddInt64(&f.processMessageCount, 1)
	f.reportLock.Lock()
	f.processDuration += time.Since(start).Nanoseconds()
	f.processSamples++
	f.reportLock.Unlock()

	return nil
}

func (f *GraphiteFilter) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&f.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageDiscards", atomic.LoadInt64(&f.processMessageDiscards), "count")
	var avgDuration int64
	f.reportLock.RLock()
	if f.processSamples > 0 {
		avgDuration = f.processDuration / f.processSamples
	}
	f.reportLock.RUnlock()
	message.NewInt64Field(msg, "ProcessMessageAvgDuration", avgDuration, "ns")
	return nil
}

func init() {
	pipeline.RegisterPlugin("WebRouterGraphiteFilter", func() interface{} {
		return new(GraphiteFilter)
	})
}
