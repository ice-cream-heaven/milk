package mq_test

import (
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/milk/mq"
	"github.com/ice-cream-heaven/utils/common"
	"os"
	"testing"
	"time"
)

type User struct {
	Name string `json:"name"`
}

func TestManager(t *testing.T) {
	_ = os.RemoveAll("Z:\\mq")

	mq.DefaultOptions.DataPath = "Z:\\mq"

	topic := mq.NewTopic[*User]("default")
	topic.GetOrCreateChannel(&mq.ChannelOption{Name: "", MaxAttempts: 3}).
		Do(func(m *mq.Message, v *User) error {
			log.Info(m)
			return common.ErrRetry("")
		})

	topic.Put(&User{Name: "test"})

	time.Sleep(time.Second * 20)
}
