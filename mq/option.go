package mq

import "time"

type TopicOption struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty" validate:"required"`
}

type ChannelOption struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty" toml:"name,omitempty" validate:"required"`

	// MaxAttempts 为 0 时，表示不限制重试次数
	MaxAttempts int64 `json:"max_attempts,omitempty" yaml:"max_attempts,omitempty" toml:"max_attempts,omitempty"`

	// Expire 为 0 时，表示不过期
	Expire time.Duration `json:"expire,omitempty" yaml:"expire,omitempty" toml:"expire,omitempty"`

	// MaxProcess 为 0 时，则限制为 1 个进程处理
	MaxProcess int64 `json:"max_process,omitempty" yaml:"max_process,omitempty" toml:"max_process,omitempty"`
}
