package mq

import (
	"github.com/elliotchance/pie/v2"
	"github.com/ice-cream-heaven/log"
	"github.com/ice-cream-heaven/utils/json"
	"github.com/nsqio/nsq/nsqd"
	"sync"
	"time"
)

type topic struct {
	sync.RWMutex
	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty" validate:"required"`

	channels map[string]*channel

	init  sync.Once
	topic *nsqd.Topic
}

func (p *topic) initNsq() {
	initOnce.Do(initNsq)

	p.topic = producer.GetTopic(p.Name)
}

func (p *topic) String() string {
	return p.Name
}

func (p *topic) toNsqMsg(m *Message) *nsqd.Message {
	p.init.Do(p.initNsq)

	return nsqd.NewMessage(p.topic.GenerateID(), json.MustMarshal(m))
}

func (p *topic) Put(m *Message) {
	p.init.Do(p.initNsq)

	_ = p.topic.PutMessage(p.toNsqMsg(m))
}

func (p *topic) MultiPut(ms []*Message) {
	p.init.Do(p.initNsq)

	_ = p.topic.PutMessages(pie.Map(ms, p.toNsqMsg))
}

func (p *topic) GetChannel(name string) *channel {
	p.RLock()
	defer p.RUnlock()

	return p.channels[name]
}

func (p *topic) GetOrCreateChannel(opt *ChannelOption) *channel {
	p.init.Do(p.initNsq)

	name := opt.Name
	if name == "" {
		name = "default"
	}

	p.RLock()
	ch, ok := p.channels[name]
	p.RUnlock()

	if ok {
		return ch
	}

	p.Lock()
	defer p.Unlock()

	ch, ok = p.channels[name]
	if ok {
		return ch
	}

	ch = newChannel(p, opt)
	p.channels[name] = ch

	return ch
}

type Topic[M any] struct {
	sync.RWMutex

	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty" validate:"required"`

	topic *topic
}

func (p *Topic[M]) GetOrCreateChannel(opt *ChannelOption) *Channel[M] {
	c := &Channel[M]{
		Name: opt.Name,

		channel: p.topic.GetOrCreateChannel(opt),
	}

	return c
}

func (p *Topic[M]) encode(v M) *Message {
	m := &Message{
		CreatedAt:   time.Now().Unix(),
		StartAt:     0,
		ExpireAt:    0,
		MaxAttempts: 0,
		TraceId:     log.GenTraceId(),
	}

	m.Data = json.MustMarshal(v)

	return m
}

func (p *Topic[M]) Put(v M) {
	p.topic.Put(p.encode(v))
}

func (p *Topic[M]) PutWithTimeout(v M, timeout time.Duration) {
	m := p.encode(v)
	m.ExpireAt = time.Now().Add(timeout).Unix()
	p.topic.Put(m)
}

func (p *Topic[M]) MultiPut(vs []M) {
	p.topic.MultiPut(pie.Map(vs, func(v M) *Message {
		return p.encode(v)
	}))
}

func (p *Topic[M]) DeferredPut(delay time.Duration, v M) {
	p.Lock()
	defer p.Unlock()

	m := p.encode(v)
	m.StartAt = time.Now().Unix() + int64(delay.Seconds())
	p.topic.Put(m)
}

func (p *Topic[M]) Depth() int64 {
	p.topic.init.Do(p.topic.initNsq)

	return p.topic.topic.Depth()
}

func (p *Topic[M]) DeleteExistingChannel(name string) {
	p.topic.init.Do(p.topic.initNsq)

	if name == "" {
		name = "default"
	}

	_ = p.topic.topic.DeleteExistingChannel(name)
}

func NewTopic[M any](name string) *Topic[M] {
	p := &Topic[M]{
		Name: name,
		topic: &topic{
			Name:     name,
			channels: make(map[string]*channel),
		},
	}

	return p
}
