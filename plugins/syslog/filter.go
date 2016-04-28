package syslog

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"github.com/laincloud/hekalain/lainlet"
)

type LainProcInfo struct {
	AppName     string `json:"app"`
	ProcName    string `json:"proc"`
	NodeName    string `json:"nodename"`
	NodeIP      string `json:"nodeip"`
	ContainerIP string `json:"ip"`
	Port        int    `json:"port"`
	InstanceNo  int    `json:"instanceNo"`
}

type SyslogLainFilterConfig struct {
	MsgType        string `toml:"message_type"`
	LainletAddress string `toml:"lainlet_address"`
	NodeName       string `toml:"nodename"`
}

type SyslogLainFilter struct {
	fr       pipeline.FilterRunner
	h        pipeline.PluginHelper
	msgType  string
	nodeName string

	dataLock sync.RWMutex
	procData map[string]LainProcInfo
	lastHash uint64

	lainletClient *lainlet.Client
	monitorCtx    context.Context
	cancelMonitor context.CancelFunc
	stop          chan bool

	reportLock             sync.RWMutex
	processDuration        int64
	processSamples         int64
	processMessageCount    int64
	processMessageDiscards int64
}

func (f *SyslogLainFilter) ConfigStruct() interface{} {
	conf := &SyslogLainFilterConfig{
		MsgType:        "lain.docker_syslog",
		LainletAddress: ":9001",
	}
	if nodeName, err := os.Hostname(); err == nil {
		conf.NodeName = nodeName
	}
	return conf
}

func (f *SyslogLainFilter) Init(config interface{}) error {
	if conf, ok := config.(*SyslogLainFilterConfig); ok {
		f.msgType = conf.MsgType
		f.nodeName = conf.NodeName
		f.lainletClient = lainlet.NewClient(conf.LainletAddress)
		f.monitorCtx, f.cancelMonitor = context.WithCancel(context.Background())
		f.stop = make(chan bool)
		return nil
	} else {
		return fmt.Errorf("Invalid syslog lain filter config object.")
	}
}

func (f *SyslogLainFilter) Prepare(fr pipeline.FilterRunner, h pipeline.PluginHelper) error {
	f.fr = fr
	f.h = h
	f.procData = make(map[string]LainProcInfo)
	f.lastHash = 0
	go f.monitorLainletEvents()
	return nil
}

func (f *SyslogLainFilter) CleanUp() {
	f.fr.LogMessage("SyslogLainFilter got command to clean up")
	close(f.stop)
	f.cancelMonitor()
	f.monitorCtx, f.cancelMonitor = context.WithCancel(context.Background())
	f.stop = make(chan bool)
	f.procData = make(map[string]LainProcInfo)
	f.lastHash = 0
}

var (
	dockerLogTagRe = regexp.MustCompile(`^docker/(\w{12})(.*)$`)
)

func (f *SyslogLainFilter) ProcessMessage(pack *pipeline.PipelinePack) error {
	defer func() {
		f.fr.UpdateCursor(pack.QueueCursor)
	}()

	start := time.Now()
	containerId := ""
	logFile := ""
	if pnValue, ok := pack.Message.GetFieldValue("programname"); ok {
		if progName, ok := pnValue.(string); ok {
			matches := dockerLogTagRe.FindStringSubmatch(progName)
			if len(matches) == 2 || len(matches) == 3 {
				containerId = matches[1]
				if len(matches) == 3 {
					_, logFile = path.Split(matches[2])
				}
			}
		}
	}
	if containerId == "" {
		atomic.AddInt64(&f.processMessageDiscards, 1)
		return nil
	}

	f.dataLock.RLock()
	procInfo, ok := f.procData[containerId]
	f.dataLock.RUnlock()
	if !ok {
		atomic.AddInt64(&f.processMessageDiscards, 1)
		return nil
	}

	msgLoopCount := pack.MsgLoopCount
	if newPack, err := f.h.PipelinePack(msgLoopCount); err != nil {
		// We should retry to create new pack
		return pipeline.NewRetryMessageError("Cannot pipeline pack, %s", err)
	} else {
		newPack.Message = message.CopyMessage(pack.Message)
		newPack.Message.SetLogger(f.fr.Name())
		newPack.Message.SetType(f.msgType)
		newPack.Message.SetUuid(uuid.NewRandom())
		message.NewStringField(newPack.Message, "kafka_topic", procInfo.ProcName)
		message.NewStringField(newPack.Message, "container_id", containerId)
		message.NewStringField(newPack.Message, "log_file", logFile)
		message.NewStringField(newPack.Message, "lain_app_name", procInfo.AppName)
		message.NewStringField(newPack.Message, "lain_proc_name", procInfo.ProcName)
		message.NewStringField(newPack.Message, "lain_container_ip", procInfo.ContainerIP)
		message.NewIntField(newPack.Message, "lain_instance_no", procInfo.InstanceNo, "#")
		f.fr.Inject(newPack)
		atomic.AddInt64(&f.processMessageCount, 1)
		f.reportLock.Lock()
		f.processDuration += time.Since(start).Nanoseconds()
		f.processSamples++
		f.reportLock.Unlock()
	}
	return nil
}

func (f *SyslogLainFilter) ReportMsg(msg *message.Message) error {
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

func (f *SyslogLainFilter) processLainletEvents(respChan <-chan *lainlet.Response) bool {
	for {
		select {
		case <-f.stop:
			return true
		case response, ok := <-respChan:
			if !ok {
				// response channel has been closed, need to restart this
				return false
			} else {
				data := response.Data
				if data == nil {
					break
				}
				start := time.Now()
				h := fnv.New64a()
				h.Write(data)
				dataHash := h.Sum64()
				if f.lastHash == dataHash {
					break
				}
				// we need to update
				newProcData := make(map[string]LainProcInfo)
				if err := json.Unmarshal(data, &newProcData); err != nil {
					f.fr.LogError(fmt.Errorf("Failed to decode the lainlet response, event=%s, err=%s",
						response.Event, err))
					break
				}
				parsedProcData := make(map[string]LainProcInfo)
				for key, lainProc := range newProcData {
					keys := strings.Split(key, "/")
					if len(keys) == 2 && keys[0] == f.nodeName && len(keys[1]) == 64 {
						cId := keys[1][:12]
						parsedProcData[cId] = lainProc
					}
				}
				f.dataLock.Lock()
				f.procData = parsedProcData
				f.lastHash = dataHash
				f.dataLock.Unlock()
				f.fr.LogMessage(fmt.Sprintf("Updated lain proc info, len=%d, duration=%s",
					len(parsedProcData), time.Now().Sub(start)))
			}
		}
	}
}

func (f *SyslogLainFilter) monitorLainletEvents() {
	defer func() {
		f.fr.LogMessage("SyslogLainFilter exits the lainlet events monitoring goroutine")
	}()

	link := fmt.Sprintf("/v2/containers?nodename=%s", f.nodeName)
	for {
		respChan, err := f.lainletClient.Watch(link, f.monitorCtx)
		if err != nil {
			f.fr.LogError(fmt.Errorf("Cannot watch the lainlet events, %s", err))
			time.Sleep(120 * time.Second)
			continue
		}
		if shouldExit := f.processLainletEvents(respChan); shouldExit {
			return
		}
		select {
		case <-f.stop:
			return
		default:
		}
		// we got here because response channel has been closed, need to restart the watcher
		time.Sleep(5 * time.Second)
	}
}

func init() {
	pipeline.RegisterPlugin("SyslogLainFilter", func() interface{} {
		return new(SyslogLainFilter)
	})
}
