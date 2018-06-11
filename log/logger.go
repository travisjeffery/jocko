package log

import (
	"time"

	z "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger interface {
	// Use Debug if you want your logs running in development, testing,
	// production.
	Debug(msg string, fields ...Field)
	// Use Info if you want your logs running in production.
	Info(msg string, fields ...Field)
	// Use Error if you want your logs running in production and you have an error.
	Error(msg string, fields ...Field)

	With(...Field) *logger
}

func New() *logger {
	l, _ := z.NewDevelopment(z.AddCallerSkip(1))
	return &logger{
		Logger: l,
	}
}

type logger struct {
	*z.Logger
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

type Field = zapcore.Field

func String(key string, val string) Field {
	return z.String(key, val)
}

func Int(key string, val int) Field {
	return z.Int(key, val)
}

func Int16(key string, val int16) Field {
	return z.Int16(key, val)
}

func Int32(key string, val int32) Field {
	return z.Int32(key, val)
}

func Int64(key string, val int64) Field {
	return z.Int64(key, val)
}

func Uint32(key string, val uint32) Field {
	return z.Uint32(key, val)
}

func Duration(key string, val time.Duration) Field {
	return z.Duration(key, val)
}

func Error(key string, val error) Field {
	return z.NamedError(key, val)
}

func Any(key string, val interface{}) Field {
	return z.Any(key, val)
}

func Object(key string, val interface{}) Field {
	return z.Object(key, val.(zapcore.ObjectMarshaler))
}
