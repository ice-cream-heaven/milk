package mq

import (
	"encoding/json"
	"fmt"
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/utils/common"
	"github.com/ice-cream-heaven/utils/xtime"
	"github.com/nsqio/nsq/nsqd"
	"time"
)

type channel struct {
	Name string

	opt *ChannelOption

	topic *topic

	queue *nsqd.Channel
}

func (p *channel) String() string {
	return fmt.Sprintf("%s:%s", p.topic.Name, p.Name)
}

func (p *channel) Topic() *topic {
	return p.topic
}

func (p *channel) Close() {
	p.queue.Close()
}

func newChannel(topic *topic, opt *ChannelOption) *channel {
	p := &channel{
		Name:  opt.Name,
		topic: topic,
		opt:   opt,

		queue: topic.topic.GetChannel(opt.Name),
	}

	return p
}

type Handler[M any] func(m *Message, v M) (err error)

type Channel[M any] struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty" validate:"required"`

	channel *channel
}

func (p *Channel[M]) Get() *Message {
	var err error

	nsqMsg := p.channel.queue.ReadMessage()

	if nsqMsg == nil {
		return nil
	}

	err = p.channel.queue.StartInFlightTimeout(nsqMsg, 1, xtime.Day)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil
	}

	finish := func() {
		err = p.channel.queue.FinishMessage(1, nsqMsg.ID)
		if err != nil {
			log.Errorf("err:%v", err)
			return
		}
	}

	requeue := func(timeout time.Duration) {
		err = p.channel.queue.RequeueMessage(1, nsqMsg.ID, timeout)
		if err != nil {
			log.Errorf("err:%v", err)
			return
		}
	}

	var m Message
	err = json.Unmarshal(nsqMsg.Body, &m)
	if err != nil {
		finish()
		log.Errorf("err:%v", err)
		return nil
	}

	if m.StartAt > 0 && m.StartAt < time.Now().Unix() {
		requeue(time.Until(time.Unix(m.StartAt, 0)))
		return nil
	}

	log.SetTrace(fmt.Sprintf("%s:%s:%s.%d", p.channel.Topic().Name, p.channel.Name, nsqMsg.ID, nsqMsg.Attempts))

	if m.ExpireAt > 0 && m.ExpireAt < time.Now().Unix() {
		log.Errorf("message expired at %d, id:%s", m.ExpireAt, nsqMsg.ID)
		finish()
		return nil
	}

	if m.MaxAttempts > 0 && nsqMsg.Attempts >= m.MaxAttempts {
		log.Errorf("message attempts exceeded, id:%s", nsqMsg.ID)
		finish()
		return nil
	}

	var v M
	err = json.Unmarshal(m.Data, &v)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil
	}

	m.v = v
	m.nsqMsg = nsqMsg

	return &m
}

func (p *Channel[M]) do(fn Handler[M]) {
	var err error
	go func() {
		for {
			log.SetTrace(fmt.Sprintf("%s:%s", p.channel.Topic().Name, p.channel.Name))
			m := p.Get()

			if m == nil {
				continue
			}

			traceId := m.TraceId
			if traceId != "" {
				log.SetTrace(traceId)
			}

			log.Debugf("exec message, id:%s", m.nsqMsg.ID)

			m.nsqMsg.Attempts++

			var retry bool
			var delay time.Duration
			err = fn(m, m.v.(M))
			if err != nil {
				log.Errorf("err:%v", err)

				switch x := err.(type) {
				case *common.Error:
					if x.NeedRetry() {
						retry = true
						if x.SkipRetryCount {
							m.nsqMsg.Attempts--
						}
						delay = x.RetryDelay
						if delay == 0 {
							delay = 3 * time.Second
						}
					}
				}
			}

			if retry {
				err = p.channel.queue.RequeueMessage(1, m.nsqMsg.ID, delay)
				if err != nil {
					log.Errorf("err:%v", err)
					continue
				}
			} else {
				err = p.channel.queue.FinishMessage(1, m.nsqMsg.ID)
				if err != nil {
					log.Errorf("err:%v", err)
					continue
				}
			}
		}
	}()
}

func (p *Channel[M]) Do(fn Handler[M]) {
	if p.channel.opt.MaxProcess == 0 {
		p.do(fn)
		return
	}

	for i := int64(0); i < p.channel.opt.MaxProcess; i++ {
		p.do(fn)
	}
}
