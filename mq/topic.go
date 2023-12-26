package mq

import (
	"github.com/elliotchance/pie/v2"
	"github.com/ice-cream-heaven/utils/json"
	"sync"
	"time"
)

type topic struct {
	sync.RWMutex
	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty" validate:"required"`

	channels map[string]*channel

	manager *Manager
}

func (p *topic) String() string {
	return p.Name
}

func (p *topic) Manager() *Manager {
	return p.manager
}

func (p *topic) Put(m *Message) {
	p.RLock()
	defer p.RUnlock()

	for _, ch := range p.channels {
		ch.Put(m)
	}
}

func (p *topic) MultiPut(ms []*Message) {
	p.RLock()
	defer p.RUnlock()
	for _, ch := range p.channels {
		ch.MultiPut(ms)
	}
}

func (p *topic) Sync() {
	p.RLock()
	defer p.RUnlock()
	for _, ch := range p.channels {
		ch.Sync()
	}
}

func (p *topic) GetChannel(name string) *channel {
	p.RLock()
	defer p.RUnlock()

	return p.channels[name]
}

func (p *topic) GetOrCreateChannel(opt *ChannelOption) *channel {
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

func (p *topic) Close() {
	p.Lock()
	defer p.Unlock()

	for _, ch := range p.channels {
		ch.Close()
	}
}

func newTopic(manager *Manager, opt *TopicOption) *topic {
	p := &topic{
		Name:     opt.Name,
		manager:  manager,
		channels: make(map[string]*channel),
	}

	return p
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
		Id:        time.Now().UnixNano(),
		CreatedAt: time.Now().Unix(),
	}

	m.Data = json.MustMarshal(v)

	return m
}

func (p *Topic[M]) Put(v M) {
	p.Lock()
	defer p.Unlock()

	p.topic.Put(p.encode(v))
}

func (p *Topic[M]) MultiPut(vs []M) {
	p.Lock()
	defer p.Unlock()

	vv := pie.Map(vs, func(v M) *Message {
		return p.encode(v)
	})

	p.topic.MultiPut(vv)
}

func (p *Topic[M]) DeferredPut(delay time.Duration, v M) {
	p.Lock()
	defer p.Unlock()

	m := p.encode(v)
	m.StartAt = time.Now().Unix() + int64(delay.Seconds())
	p.topic.Put(m)
}

func NewTopic[M any](name string) *Topic[M] {
	p := &Topic[M]{
		Name: name,
		topic: manager.GetOrCreateTopic(&TopicOption{
			Name: name,
		}),
	}

	return p
}
