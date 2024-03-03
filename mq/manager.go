package mq

import (
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/utils/app"
	"github.com/ice-cream-heaven/utils/osx"
	"github.com/ice-cream-heaven/utils/routine"
	"github.com/ice-cream-heaven/utils/runtime"
	"github.com/ice-cream-heaven/utils/unit"
	"github.com/ice-cream-heaven/utils/xtime"
	"github.com/nsqio/nsq/nsqd"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	DefaultOptions = nsqd.NewOptions()

	initOnce sync.Once

	producer *nsqd.NSQD
)

func init() {
	DefaultOptions.HTTPAddress = ""
	DefaultOptions.HTTPSAddress = ""
	DefaultOptions.TCPAddress = ""
	DefaultOptions.BroadcastAddress = ""
	DefaultOptions.MemQueueSize = 10
	DefaultOptions.DataPath = filepath.Join(os.TempDir(), "ice", "mq", app.Name)
	DefaultOptions.MaxMsgSize = unit.GB
	DefaultOptions.SyncEvery = 5000
	DefaultOptions.SyncTimeout = xtime.Second
	DefaultOptions.MsgTimeout = xtime.Week
	DefaultOptions.MaxBytesPerFile = unit.GB * 4

	DefaultOptions.Logger = NewLogger()
}

func initNsq() {
	var err error

	if !osx.IsDir(DefaultOptions.DataPath) {
		err = os.MkdirAll(DefaultOptions.DataPath, 0666)
		if err != nil {
			log.Panicf("err:%v", err)
			return
		}
	}

	producer, err = nsqd.New(DefaultOptions)
	if err != nil {
		log.Panicf("err:%v", err)
		return
	}

	routine.Go(func() (err error) {
		err = producer.Main()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		return nil
	})

	go func(c chan os.Signal) {
		for {
			select {
			case <-c:
				return
			case <-time.After(time.Minute * 5):
				err := filepath.Walk(DefaultOptions.DataPath, func(path string, info fs.FileInfo, err error) error {
					if info == nil {
						return nil
					}

					if info.IsDir() {
						return nil
					}

					switch filepath.Ext(path) {
					case ".bad":
						log.Warnf("remove bad file:%s", path)
						err := os.RemoveAll(path)
						if err != nil {
							log.Errorf("err:%v", err)
						}
					case ".tmp":
						if time.Since(info.ModTime()) > xtime.Minute*5 {
							log.Warnf("remove bad file:%s", path)
							err := os.RemoveAll(path)
							if err != nil {
								log.Errorf("err:%v", err)
							}
						}
					}
					return nil
				})
				if err != nil {
					log.Errorf("err:%v", err)
				}
			}
		}
	}(runtime.GetExitSign())
}

func Close() {
	for topicName, topic := range producer.CloneTopic() {
		for channelName, channel := range topic.CloneChannel() {
			err := channel.Close()
			if err != nil {
				log.Errorf("close channel %s-%s err:%v", topicName, channelName, err)
			}
		}

		err := topic.Close()
		if err != nil {
			log.Errorf("close topic %s err:%v", topicName, err)
		}
	}
}

func Topics() []*nsqd.Topic {
	var topics []*nsqd.Topic

	for _, topic := range producer.CloneTopic() {
		topics = append(topics, topic)
	}

	return topics
}
