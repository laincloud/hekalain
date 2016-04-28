package webrouter

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"github.com/laincloud/hekalain/pkgs/hekahelper"
)

type KafkaFilterConfig struct {
	OutputMsgType string `toml:"output_message_type"`
}

type KafkaFilter struct {
	fr      pipeline.FilterRunner
	h       pipeline.PluginHelper
	msgType string

	cancelMonitor context.CancelFunc

	reportLock             sync.RWMutex
	processDuration        int64
	processSamples         int64
	processMessageCount    int64
	processMessageDiscards int64
}

func (f *KafkaFilter) ConfigStruct() interface{} {
	conf := &KafkaFilterConfig{
		OutputMsgType: "webrouter.kafka.access",
	}
	return conf
}

func (f *KafkaFilter) Init(config interface{}) error {
	if conf, ok := config.(*KafkaFilterConfig); ok {
		_, f.cancelMonitor = context.WithCancel(context.Background())
		f.msgType = conf.OutputMsgType
		return nil
	}
	return fmt.Errorf("Invalid WebRouterKafkaFilter config object.")
}

func (f *KafkaFilter) Prepare(fr pipeline.FilterRunner, h pipeline.PluginHelper) error {
	f.fr = fr
	f.h = h
	return nil
}

func (f *KafkaFilter) CleanUp() {
	f.fr.LogMessage("WebRouterKafkaFilter got command to clean up")
	f.cancelMonitor()
	_, f.cancelMonitor = context.WithCancel(context.Background())
}

func (f *KafkaFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
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

	newPack.Message = message.CopyMessage(msg)
	newPack.Message.SetLogger(f.fr.Name())
	newPack.Message.SetType(f.msgType)
	newPack.Message.SetUuid(uuid.NewRandom())
	host, _ := hekahelper.GetMessageStringValue(msg, "Host")
	message.NewStringField(newPack.Message, "kafka_topic", fmt.Sprintf("%s.access", host))
	f.fr.Inject(newPack)
	atomic.AddInt64(&f.processMessageCount, 1)
	f.reportLock.Lock()
	f.processDuration += time.Since(start).Nanoseconds()
	f.processSamples++
	f.reportLock.Unlock()
	return nil
}

func (f *KafkaFilter) ReportMsg(msg *message.Message) error {
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
	pipeline.RegisterPlugin("WebRouterKafkaFilter", func() interface{} {
		return new(KafkaFilter)
	})
}
