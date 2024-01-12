package mq

import (
	"github.com/ice-cream-heaven/log"
	"strings"
)

type Logger struct {
}

func NewLogger() *Logger {
	return &Logger{}
}

func (l *Logger) Output(maxdepth int, s string) error {
	var level log.Level
	idx := strings.Index(s, ": ")
	if idx > 0 {
		switch s[:idx] {
		case "ERROR":
			level = log.ErrorLevel

		case "WARN":
			level = log.WarnLevel

		case "INFO":
			level = log.InfoLevel

		case "DEBUG":
			level = log.DebugLevel

		default:
			level = log.InfoLevel

		}
		s = s[idx+2:]
	}

	log.Log(level, s)

	return nil
}
