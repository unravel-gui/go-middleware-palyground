package logger

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// Logger 接口定义了日志组件的基本方法
type Logger interface {
	log(level LogLevel, format string, args ...interface{})
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Fatal(err error)
	Fatalf(format string, args ...interface{})
	SetLevel(flag LogLevel)
	levelToString(level LogLevel) string
}

type BasicLogger struct {
	mu sync.Mutex
	// 可以添加其他配置项
	LoggerLevel LogLevel
	endpoint    string
}

var _ Logger = (*BasicLogger)(nil)
var DLogger Logger // 全局日志对象

func NewBasicLogger(logLeverl LogLevel, endpoint string) *BasicLogger {
	basicLogger := new(BasicLogger)
	basicLogger.LoggerLevel = logLeverl
	basicLogger.endpoint = endpoint
	return basicLogger
}

// Debug 实现了 Logger 接口的 Debug 方法
func (l *BasicLogger) Debug(format string, args ...interface{}) {
	if l.LoggerLevel == DEBUG {
		l.log(DEBUG, format, args...)
	}
}

// Info 实现了 Logger 接口的 Info 方法
func (l *BasicLogger) Info(format string, args ...interface{}) {
	if l.LoggerLevel <= INFO {
		l.log(INFO, format, args...)
	}
}

// Warn 实现了 Logger 接口的 Warn 方法
func (l *BasicLogger) Warn(format string, args ...interface{}) {
	if l.LoggerLevel <= WARN {
		l.log(WARN, format, args...)
	}
}

func (l *BasicLogger) Error(format string, args ...interface{}) {
	if l.LoggerLevel <= ERROR {
		l.log(ERROR, format, args...)
	}
}
func (l *BasicLogger) Fatal(err error) {
	if l.LoggerLevel <= FATAL {
		l.log(FATAL, err.Error())
		os.Exit(1)
	}
}

func (l *BasicLogger) Fatalf(format string, args ...interface{}) {
	if l.LoggerLevel <= FATAL {
		l.log(FATAL, format, args...)
		os.Exit(1)
	}
}

// log 是一个内部方法，用于实际记录日志
func (l *BasicLogger) log(level LogLevel, format string, args ...interface{}) {
	levelStr := l.levelToString(level)
	format = fmt.Sprintf("[%s]:  [%s] %s\n", l.endpoint, levelStr, format)
	log.Printf(format, args...)
}

func (l *BasicLogger) levelToString(level LogLevel) string {
	switch level {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

func (l *BasicLogger) SetLevel(flag LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.LoggerLevel = flag
}
