package mq

import (
	"github.com/ice-cream-heaven/utils/app"
	"os"
	"path/filepath"
	"sync"
)

type Manager struct {
	sync.RWMutex

	topics map[string]*topic

	dataDirectory string `json:"data_directory,omitempty" yaml:"data_directory,omitempty" toml:"data_directory,omitempty" validate:"required"`
}

func (p *Manager) GetTopic(name string) *topic {
	p.RLock()
	defer p.RUnlock()

	return p.topics[name]
}

func (p *Manager) GetDataDirectory() string {
	return p.dataDirectory
}

func (p *Manager) GetOrCreateTopic(opt *TopicOption) *topic {
	name := opt.Name
	if name == "" {
		panic("topic name is empty")
	}

	p.RLock()
	topic, ok := p.topics[name]
	p.RUnlock()

	if ok {
		return topic
	}

	p.Lock()
	defer p.Unlock()

	topic, ok = p.topics[name]
	if ok {
		return topic
	}

	topic = newTopic(p, opt)
	p.topics[name] = topic

	return topic
}

func (p *Manager) Close() {
	p.Lock()
	defer p.Unlock()

	for _, topic := range p.topics {
		topic.Close()
	}
}

func (p *Manager) SetDataDirectory(dataDirectory string) {
	p.dataDirectory = dataDirectory
}

func NewManager(dataDirectory string) *Manager {
	if dataDirectory == "" {
		dataDirectory = filepath.Join(os.TempDir(), "ice", "mq", app.Name)
	}

	p := &Manager{
		topics:        make(map[string]*topic),
		dataDirectory: dataDirectory,
	}

	return p
}

var manager = NewManager("")

func SetDataDirectory(dataDirectory string) {
	manager.SetDataDirectory(dataDirectory)
}
