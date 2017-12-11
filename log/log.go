package log

import (
	"go.uber.org/zap"
)

type Option = zap.Option

type Logger interface {
	// Debug is used when you want your logs running in development, testing,
	// production.
	Debug(msg string, fields ...Field)
	// Info is used when you want your logs running in production.
	Info(msg string, fields ...Field)
	// Error is used when you want your logs running in production and you have an error.
	Error(msg string, fields ...Field)
	/// With returns a child logger wrapped with the given fields.
	With(fields ...Field) Logger
	WithOptions(opts ...Option) Logger
}

var _ Logger = (*logger)(nil)

func New() *logger {
	l, _ := zap.NewDevelopment(zap.AddCallerSkip(1))
	return &logger{
		Logger: l,
	}
}

type logger struct {
	*zap.Logger
}

func (l *logger) Info(msg string, fields ...Field) {
	l.Logger.Info(msg, fields...)
}

func (l *logger) Error(msg string, fields ...Field) {
	l.Logger.Error(msg, fields...)
}

func (l *logger) Debug(msg string, fields ...Field) {
	l.Logger.Debug(msg, fields...)
}

func (l *logger) With(fields ...Field) Logger {
	return &logger{
		Logger: l.Logger.With(fields...),
	}
}

func (l *logger) WithOptions(opts ...Option) Logger {
	return &logger{
		Logger: l.Logger.WithOptions(opts...),
	}
}
