package mq

import (
	"encoding/json"
	"fmt"
	"github.com/elliotchance/pie/v2"
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/utils/common"
	"time"
)

type channel struct {
	Name string

	opt *ChannelOption

	topic *topic

	queue *diskQueue
}

func (p *channel) String() string {
	return fmt.Sprintf("%s:%s", p.topic.Name, p.Name)
}

func (p *channel) Topic() *topic {
	return p.topic
}

func (p *channel) Manager() *Manager {
	return p.topic.Manager()
}

func (p *channel) Queue() *diskQueue {
	return p.queue
}

func (p *channel) Get() *Message {
	return p.queue.Get()
}

func (p *channel) GetWithTimeout(timeout time.Duration) *Message {
	return p.queue.GetWithTimeout(timeout)
}

func (p *channel) Put(m *Message) {
	if p.opt.Expire > 0 {
		m.ExpireAt = time.Now().Unix() + p.opt.Expire
	} else {
		m.ExpireAt = 0
	}

	if p.opt.MaxAttempts > 0 {
		m.MaxAttempts = p.opt.MaxAttempts
	} else {
		m.MaxAttempts = 0
	}

	p.queue.Put(m)
}

func (p *channel) RePut(m *Message) {
	log.Info("requeue message")
	m.Attempts++
	p.queue.Put(m)
}

func (p *channel) MultiPut(ms []*Message) {
	pie.Each(ms, func(m *Message) {
		p.Put(m)
	})
}

func (p *channel) Close() {
	p.queue.Close()
}

func (p *channel) Sync() {
	p.queue.Sync()
}

func newChannel(topic *topic, opt *ChannelOption) *channel {
	p := &channel{
		Name:  opt.Name,
		topic: topic,
		opt:   opt,
	}

	p.queue = newDiskQueueWithChanel(p)
	p.queue.Loop()

	return p
}

type Handler[M any] func(m *Message, v M) error

type Channel[M any] struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty" validate:"required"`

	channel *channel
}

func (p *Channel[M]) Get() *Message {
	m := p.channel.Get()

	if m == nil {
		return nil
	}

	if m.StartAt > 0 && m.StartAt < time.Now().Unix() {
		m.Attempts--
		p.channel.RePut(m)
		return nil
	}

	log.SetPrefixMsg(fmt.Sprintf("%s:%s:%d.%d", p.channel.Topic().Name, p.channel.Name, m.Id, m.Attempts))

	if m.ExpireAt > 0 && m.ExpireAt < time.Now().Unix() {
		log.Errorf("message expired, id:%d", m.Id)
		return nil
	}

	if m.MaxAttempts > 0 && m.Attempts >= m.MaxAttempts {
		log.Errorf("message attempts exceeded, id:%d", m.Id)
		return nil
	}

	var v M
	err := json.Unmarshal(m.Data, &v)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil
	}

	m.v = v

	return m
}

func (p *Channel[M]) do(fn Handler[M]) {
	var err error
	go func() {
		for {
			m := p.Get()

			if m == nil {
				continue
			}

			log.Infof("exec message, id:%d", m.Id)

			err = fn(m, m.v.(M))
			if err != nil {
				log.Errorf("err:%v", err)

				switch x := err.(type) {
				case *common.Error:
					if x.SkipRetryCount {
						m.Attempts--
					}

					if x.RetryDelay > 0 {
						m.StartAt = time.Now().Unix() + int64(x.RetryDelay.Seconds())
					} else {
						m.StartAt = time.Now().Unix() + 5
					}

					if x.Retry {
						p.channel.RePut(m)
					}
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
