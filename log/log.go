package log

import (
	stdlog "log"

	"upspin.io/log"
)

var (
	Debug = &logger{l: log.Debug}
	Info  = &logger{l: log.Info}
	Error = &logger{l: log.Error}
)

type logger struct {
	prefix string
	l      log.Logger
}

type Level = log.Level

var (
	DebugLevel = log.DebugLevel
	InfoLevel  = log.InfoLevel
	ErrorLevel = log.ErrorLevel
)

func New(level log.Level, prefix string) *logger {
	l := &logger{prefix: prefix}
	switch level {
	case log.DebugLevel:
		l.l = log.Debug
	case log.InfoLevel:
		l.l = log.Info
	case log.ErrorLevel:
		l.l = log.Error
	}
	return l
}

func SetPrefix(prefix string) {
	for _, logger := range []*logger{Debug, Info, Error} {
		logger.prefix = prefix
	}
}

func SetLevel(level string) {
	log.SetLevel(level)
}

func NewStdLogger(l log.Logger) *stdlog.Logger {
	return log.NewStdLogger(l)
}

func (l *logger) Printf(format string, v ...interface{}) {
	if l.prefix == "" {
		l.l.Printf(format, v...)
	} else {
		l.l.Printf(l.prefix+format, v...)
	}
}

func (l *logger) Print(v ...interface{}) {
	if l.prefix == "" {
		l.l.Print(v...)
	} else {
		l.l.Print(append([]interface{}{l.prefix}, v...)...)
	}
}

func (l *logger) Println(v ...interface{}) {
	if l.prefix == "" {
		l.l.Println(v...)
	} else {
		l.l.Println(append([]interface{}{l.prefix}, v...)...)
	}
}

func (l *logger) Fatal(v ...interface{}) {
	if l.prefix == "" {
		l.l.Fatal(v...)
	} else {
		l.l.Fatal(append([]interface{}{l.prefix}, v...)...)
	}
}

func (l *logger) Fatalf(format string, v ...interface{}) {
	if l.prefix == "" {
		l.l.Fatalf(format, v...)
	} else {
		l.l.Fatalf(l.prefix+format, v...)
	}
}

var _ log.Logger = (*logger)(nil)
