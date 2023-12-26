package mq

import (
	"github.com/ice-cream-heaven/log"
	"strconv"
)

type Message struct {
	Id        int64 `json:"id,omitempty" yaml:"id,omitempty" toml:"id,omitempty" validate:"required"`
	CreatedAt int64 `json:"created_at,omitempty" yaml:"created_at,omitempty" toml:"created_at,omitempty"`

	// StartAt 为 0 时，表示立即开始
	StartAt int64 `json:"start_at,omitempty" yaml:"start_at,omitempty" toml:"start_at,omitempty"`

	// ExpireAt 为 0 时，表示不过期
	ExpireAt int64 `json:"expire_at,omitempty" yaml:"expire_at,omitempty" toml:"expire_at,omitempty"`

	Attempts int64 `json:"attempts,omitempty" yaml:"attempts,omitempty" toml:"attempts,omitempty"`

	MaxAttempts int64 `json:"max_attempts,omitempty" yaml:"max_attempts,omitempty" toml:"max_attempts,omitempty"`

	Data []byte `json:"data,omitempty" yaml:"data,omitempty" toml:"data,omitempty"`

	v interface{}
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

func (p *Message) Encode() []byte {
	b := log.GetBuffer()
	defer log.PutBuffer(b)

	b.WriteString(strconv.FormatInt(p.Id, 10))
	b.WriteByte('|')

	b.WriteString(strconv.FormatInt(p.StartAt, 10))
	b.WriteByte('|')

	b.WriteString(strconv.FormatInt(p.ExpireAt, 10))
	b.WriteByte('|')

	b.WriteString(strconv.FormatInt(p.CreatedAt, 10))
	b.WriteByte('|')

	b.WriteString(strconv.FormatInt(p.Attempts, 10))
	b.WriteByte('|')

	b.WriteString(strconv.FormatInt(p.MaxAttempts, 10))
	b.WriteByte('|')

	b.Write(p.Data)
	return b.Bytes()
}

func MessageDecode(b []byte) (*Message, error) {
	var err error

	p := &Message{}

	bb := log.GetBuffer()
	defer log.PutBuffer(bb)

	var i int
	for ; i < len(b); i++ {
		if b[i] == '|' {
			i++
			break
		}
		bb.WriteByte(b[i])
	}

	p.Id, err = strconv.ParseInt(
		bb.String(), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	bb.Reset()
	for ; i < len(b); i++ {
		if b[i] == '|' {
			i++
			break
		}
		bb.WriteByte(b[i])
	}

	p.StartAt, err = strconv.ParseInt(bb.String(), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	bb.Reset()
	for ; i < len(b); i++ {
		if b[i] == '|' {
			i++
			break
		}
		bb.WriteByte(b[i])
	}

	p.ExpireAt, err = strconv.ParseInt(bb.String(), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	bb.Reset()
	for ; i < len(b); i++ {
		if b[i] == '|' {
			i++
			break
		}
		bb.WriteByte(b[i])
	}

	p.CreatedAt, err = strconv.ParseInt(bb.String(), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	bb.Reset()
	for ; i < len(b); i++ {
		if b[i] == '|' {
			i++
			break
		}
		bb.WriteByte(b[i])
	}

	p.Attempts, err = strconv.ParseInt(bb.String(), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	bb.Reset()
	for ; i < len(b); i++ {
		if b[i] == '|' {
			i++
			break
		}
		bb.WriteByte(b[i])
	}

	p.MaxAttempts, err = strconv.ParseInt(bb.String(), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	p.Data = b[i:]

	return p, nil
}
