package mq_test

import (
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/milk/mq"
	"github.com/ice-cream-heaven/utils/unit"
	"os"
	"sync"
	"testing"
)

type User struct {
	Name string `json:"name"`
}

func TestManager(t *testing.T) {
	_ = os.RemoveAll("Z:\\mq")

	mq.DefaultOptions.DataPath = "Z:\\mq"
	mq.DefaultOptions.MemQueueSize = 0
	mq.DefaultOptions.MaxBytesPerFile = unit.MB * 100

	topic := mq.NewTopic[*User]("default")

	var w sync.WaitGroup

	for i := 0; i < 10; i++ {
		w.Add(1)
		topic.Put(&User{Name: "test"})
	}

	topic.GetOrCreateChannel(&mq.ChannelOption{Name: "", MaxAttempts: 3}).
		Do(func(m *mq.Message, v *User) error {
			defer w.Done()
			log.Info(m)
			return nil
		})

	w.Wait()
}
