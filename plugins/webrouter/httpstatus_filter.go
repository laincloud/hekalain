package webrouter

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"github.com/laincloud/hekalain/pkgs/hekahelper"
)

type HTTPStatusStat struct {
	HTTP500       int64   `json:"http_500"`
	HTTP501       int64   `json:"http_501"`
	HTTP502       int64   `json:"http_502"`
	HTTP503       int64   `json:"http_503"`
	HTTP504       int64   `json:"http_504"`
	HTTP505       int64   `json:"http_505"`
	AvgTimeCost   float64 `json:"avg_reqtime"`
	QueriesPerMin float64 `json:"qpm"`
	totalTimeCost float64
	counts        int
}

type HTTPStatusFilterConfig struct {
	OutputMsgType  string `toml:"output_message_type"`
	TickerInterval uint   `toml:"ticker_interval"`
}

type HTTPStatusFilter struct {
	fr             pipeline.FilterRunner
	h              pipeline.PluginHelper
	msgType        string
	tickerInterval uint

	cancelMonitor context.CancelFunc

	reportLock             sync.RWMutex
	processDuration        int64
	processSamples         int64
	processMessageCount    int64
	processMessageDiscards int64
	httpStatusStats        map[string]*HTTPStatusStat
}

func (f *HTTPStatusFilter) ConfigStruct() interface{} {
	conf := &HTTPStatusFilterConfig{
		OutputMsgType:  "http_status.stat",
		TickerInterval: 60,
	}
	return conf
}

func (f *HTTPStatusFilter) Init(config interface{}) error {
	f.httpStatusStats = make(map[string]*HTTPStatusStat)
	if conf, ok := config.(*HTTPStatusFilterConfig); ok {
		_, f.cancelMonitor = context.WithCancel(context.Background())
		f.msgType = conf.OutputMsgType
		f.tickerInterval = conf.TickerInterval
		return nil
	}
	return fmt.Errorf("Invalid WebRouterHTTPStatusFilter config object.")
}

func (f *HTTPStatusFilter) Prepare(fr pipeline.FilterRunner, h pipeline.PluginHelper) error {
	f.fr = fr
	f.h = h
	return nil
}

func (f *HTTPStatusFilter) CleanUp() {
	f.fr.LogMessage("WebRouterHTTPStatusFilter got command to clean up")
	f.cancelMonitor()
	_, f.cancelMonitor = context.WithCancel(context.Background())
}

func (f *HTTPStatusFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	defer func() {
		f.fr.UpdateCursor(pack.QueueCursor)
	}()

	start := time.Now()
	msg := pack.Message

	status, _ := hekahelper.GetMessageDoubleValue(msg, "status")
	reqTime, _ := hekahelper.GetMessageDoubleValue(msg, "request_time")
	host, _ := hekahelper.GetMessageStringValue(msg, "Host")

	if _, exists := f.httpStatusStats[host]; !exists {
		f.httpStatusStats[host] = &HTTPStatusStat{}
	}
	stat := f.httpStatusStats[host]
	stat.counts++
	stat.totalTimeCost += reqTime
	switch int(status) {
	case 500:
		stat.HTTP500++
	case 501:
		stat.HTTP501++
	case 502:
		stat.HTTP502++
	case 503:
		stat.HTTP503++
	case 504:
		stat.HTTP504++
	case 505:
		stat.HTTP505++
	}
	atomic.AddInt64(&f.processMessageCount, 1)
	f.reportLock.Lock()
	f.processDuration += time.Since(start).Nanoseconds()
	f.processSamples++
	f.reportLock.Unlock()

	return nil
}

func (f *HTTPStatusFilter) TimerEvent() error {
	// If no data found, we should not generate a new pack
	if len(f.httpStatusStats) == 0 {
		return nil
	}

	for _, stat := range f.httpStatusStats {
		if stat.counts != 0 {
			stat.AvgTimeCost = stat.totalTimeCost / float64(stat.counts)
		}
		stat.QueriesPerMin = float64(stat.counts) / float64(f.tickerInterval) * 60
	}
	var payload []byte
	var err error
	if payload, err = json.Marshal(f.httpStatusStats); err != nil {
		return fmt.Errorf("Marshall http stat message error: %s", err.Error())
	}

	var pack *pipeline.PipelinePack

	if pack, err = f.h.PipelinePack(0); err != nil {
		// We should retry to create new pack
		return pipeline.NewRetryMessageError("WebrouterGraphite pipeline pack: %s", err.Error())
	}
	msg := pack.Message
	msg.SetUuid(uuid.NewRandom())
	msg.SetSeverity(7)
	msg.SetType(f.msgType)
	msg.SetLogger(f.fr.Name())
	msg.SetTimestamp(time.Now().UnixNano())
	hostName, _ := os.Hostname()
	msg.SetHostname(hostName)
	msg.SetPid(int32(os.Getpid()))

	msg.SetPayload(string(payload))

	for _, stat := range f.httpStatusStats {
		stat.AvgTimeCost = 0.0
		stat.HTTP500 = 0
		stat.HTTP501 = 0
		stat.HTTP502 = 0
		stat.HTTP503 = 0
		stat.HTTP504 = 0
		stat.HTTP505 = 0
		stat.QueriesPerMin = 0.0
		stat.counts = 0
		stat.totalTimeCost = 0.0
	}

	f.fr.Inject(pack)
	return nil
}

func (f *HTTPStatusFilter) ReportMsg(msg *message.Message) error {
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
	pipeline.RegisterPlugin("WebRouterHTTPStatusFilter", func() interface{} {
		return new(HTTPStatusFilter)
	})
}
