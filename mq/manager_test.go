package mq_test

import (
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/milk/mq"
	"github.com/ice-cream-heaven/utils/common"
	"testing"
	"time"
)

type User struct {
	Name string `json:"name"`
}

func TestManager(t *testing.T) {
	mq.SetDataDirectory("Z:\\mq")

	topic := mq.NewTopic[*User]("test")
	channel := topic.GetOrCreateChannel(&mq.ChannelOption{Name: "test", MaxAttempts: 3})
	channel.Do(func(m *mq.Message, v *User) error {
		log.Info(m)
		return common.ErrRetry("")
	})

	topic.Put(&User{Name: "test"})
	time.Sleep(time.Second * 20)
}
