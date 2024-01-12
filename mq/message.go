package mq

import "github.com/nsqio/nsq/nsqd"

type Message struct {
	CreatedAt int64 `json:"created_at,omitempty" yaml:"created_at,omitempty" toml:"created_at,omitempty"`

	TraceId string `json:"trace_id,omitempty" yaml:"trace_id,omitempty" toml:"trace_id,omitempty"`

	// StartAt 为 0 时，表示立即开始
	StartAt int64 `json:"start_at,omitempty" yaml:"start_at,omitempty" toml:"start_at,omitempty"`

	// ExpireAt 为 0 时，表示不过期
	ExpireAt int64 `json:"expire_at,omitempty" yaml:"expire_at,omitempty" toml:"expire_at,omitempty"`

	MaxAttempts uint16 `json:"max_attempts,omitempty" yaml:"max_attempts,omitempty" toml:"max_attempts,omitempty"`

	Data []byte `json:"data,omitempty" yaml:"data,omitempty" toml:"data,omitempty"`

	v interface{} `json:"-"`

	nsqMsg *nsqd.Message `json:"-"`
}

/*
NOTE: 消息格式
      ID
	  StartAt
	  ExpireAt
	  CreatedAt
	  Attempts
	  MaxAttempts
	  Data
*/
