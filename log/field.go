package log

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Field = zapcore.Field

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
