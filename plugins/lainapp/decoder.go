package lainapp

import (
	"fmt"
	"sync/atomic"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type LainAppRetagDecoderConfig struct {
	MsgType    string `toml:"message_type"`
	AppName    string `toml:"app_name"`
	ProcName   string `toml:"proc_name"`
	InstanceNo int    `toml:"instance_no"`
	LogFile    string `toml:"log_file"`
	KafkaTopic string `toml:"kafka_topic"`
}

func (conf LainAppRetagDecoderConfig) IsValid() bool {
	return conf.AppName != "" && conf.ProcName != "" && conf.KafkaTopic != ""
}

type LainAppRetagDecoder struct {
	config              *LainAppRetagDecoderConfig
	processMessageCount int64
}

func (d *LainAppRetagDecoder) ConfigStruct() interface{} {
	conf := &LainAppRetagDecoderConfig{
		MsgType: "lain.app_retagged_log",
	}
	return conf
}

func (d *LainAppRetagDecoder) Init(config interface{}) error {
	d.config = config.(*LainAppRetagDecoderConfig)
	if !d.config.IsValid() {
		return fmt.Errorf("Missing config detail for the decoder")
	}
	return nil
}

func (d *LainAppRetagDecoder) Decode(pack *pipeline.PipelinePack) ([]*pipeline.PipelinePack, error) {
	packs := make([]*pipeline.PipelinePack, 0, 1)
	pack.Message.SetType(d.config.MsgType)
	message.NewStringField(pack.Message, "kafka_topic", d.config.KafkaTopic)
	message.NewStringField(pack.Message, "container_id", "")
	message.NewStringField(pack.Message, "log_file", d.config.LogFile)
	message.NewStringField(pack.Message, "lain_app_name", d.config.AppName)
	message.NewStringField(pack.Message, "lain_proc_name", d.config.ProcName)
	message.NewStringField(pack.Message, "lain_container_ip", "")
	message.NewIntField(pack.Message, "lain_instance_no", d.config.InstanceNo, "#")
	packs = append(packs, pack)

	atomic.AddInt64(&d.processMessageCount, 1)
	return packs, nil
}

func (d *LainAppRetagDecoder) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount", atomic.LoadInt64(&d.processMessageCount), "count")
	return nil
}

func init() {
	pipeline.RegisterPlugin("LainAppRetagDecoder", func() interface{} {
		return new(LainAppRetagDecoder)
	})
}
