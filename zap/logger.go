package zap

import (
	"github.com/travisjeffery/jocko"
	z "go.uber.org/zap"
)

func New() *logger {
	l, _ := z.NewProduction(z.AddCallerSkip(1))
	return &logger{
		Logger: l,
	}
}

type logger struct {
	*z.Logger
}

func (l *logger) Info(msg string, fields ...jocko.Field) {
	l.Logger.Info(msg, fields...)
}

func (l *logger) Error(msg string, fields ...jocko.Field) {
	l.Logger.Error(msg, fields...)
}

func (l *logger) Debug(msg string, fields ...jocko.Field) {
	l.Logger.Debug(msg, fields...)
}
