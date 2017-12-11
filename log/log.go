package log

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Field = zapcore.Field
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

func String(key string, val string) Field {
	return zap.String(key, val)
}

func Bool(key string, val bool) Field {
	return zap.Bool(key, val)
}

func Int(key string, val int) Field {
	return zap.Int(key, val)
}

func Int16(key string, val int16) Field {
	return zap.Int16(key, val)
}

func Int32(key string, val int32) Field {
	return zap.Int32(key, val)
}

func Uint32(key string, val uint32) Field {
	return zap.Uint32(key, val)
}

func Duration(key string, val time.Duration) Field {
	return zap.Duration(key, val)
}

func Error(key string, val error) Field {
	return zap.NamedError(key, val)
}

func Any(key string, val interface{}) Field {
	return zap.Any(key, val)
}

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

func (l *logger) With(fields ...Field) *logger {
	return &logger{
		Logger: l.Logger.With(fields...),
	}
}

func (l *logger) WithOptions(opts ...Option) *logger {
	return &logger{
		Logger: l.Logger.WithOptions(opts...),
	}
}
