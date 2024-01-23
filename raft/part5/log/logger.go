package logger

import (
	"fmt"
	"log"
	"os"
	conf "raft/part5/config"
	"sync"
)

type LogLevel int

var config = conf.DefaultConfig

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	CRITICAL
	FATAL
)

// Logger 接口定义了日志组件的基本方法
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Critical(format string, args ...interface{})
	Fatal(err error)
	Fatalf(format string, args ...interface{})
	SetLogLevel(flag LogLevel)
}

type BasicLogger struct {
	logger *log.Logger
	level  LogLevel
}

var BLogger *BasicLogger
var _ Logger = (*BasicLogger)(nil)
var basicOnce sync.Once

func GetBasicLogger() *BasicLogger {
	basicOnce.Do(func() {
		// TODO：读取配置
		BLogger = newBasicLogger(0)
	})
	return BLogger
}

// NewBasicLogger 创建一个新的BasicLogger实例
func newBasicLogger(level LogLevel) *BasicLogger {
	return &BasicLogger{
		logger: log.New(os.Stdout, "", log.LstdFlags),
		level:  level,
	}
}

// SetLogLevel 设置日志等级
func (bl *BasicLogger) SetLogLevel(level LogLevel) {
	bl.level = level
}

// logMessage 封装日志消息
func (bl *BasicLogger) logMessage(level LogLevel, format string, args ...interface{}) {
	if level >= bl.level {
		message := fmt.Sprintf(format, args...)
		bl.logger.Printf("[%s]: %s", level.String(), message)
	}
}

// Debug 打印调试信息
func (bl *BasicLogger) Debug(format string, args ...interface{}) {
	bl.logMessage(DEBUG, format, args...)
}

// Info 打印信息
func (bl *BasicLogger) Info(format string, args ...interface{}) {
	bl.logMessage(INFO, format, args...)
}

// Warn 打印警告信息
func (bl *BasicLogger) Warn(format string, args ...interface{}) {
	bl.logMessage(WARN, format, args...)
}

// Error 打印错误信息
func (bl *BasicLogger) Error(format string, args ...interface{}) {
	bl.logMessage(ERROR, format, args...)
}

// Critical 打印严重问题信息
func (bl *BasicLogger) Critical(format string, args ...interface{}) {
	bl.logMessage(CRITICAL, format, args...)
}

// Fatal 打印严重问题信息
func (bl *BasicLogger) Fatal(err error) {
	bl.logMessage(FATAL, err.Error())
}

// Fatalf 打印严重问题信息
func (bl *BasicLogger) Fatalf(format string, args ...interface{}) {
	bl.logMessage(FATAL, format, args...)
}

func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case CRITICAL:
		return "CRITICAL"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

var _ Logger = (*BasicLogger)(nil)

type RaftLogger struct {
	BLogger  *BasicLogger
	endpoint string
	id       int64
}

var RLogger *RaftLogger

var _ Logger = (*RaftLogger)(nil)
var raftOnce sync.Once

func GetRaftLogger() *RaftLogger {
	raftOnce.Do(func() {
		RLogger = NewRaftLogger(config.Id, config.Endpoint)
	})
	return RLogger
}

// NewRaftLogger 创建一个新的 RaftLogger 实例
func NewRaftLogger(id int64, endpoint string) *RaftLogger {
	return &RaftLogger{
		BLogger:  GetBasicLogger(),
		endpoint: endpoint,
		id:       id,
	}
}

// Debug 打印调试信息，带有节点信息
func (rl *RaftLogger) Debug(format string, args ...interface{}) {
	rl.BLogger.Debug(rl.getNodeMessage(format), args...)
}

// Info 打印信息，带有节点信息
func (rl *RaftLogger) Info(format string, args ...interface{}) {
	rl.BLogger.Info(rl.getNodeMessage(format), args...)
}

// Warn 打印警告信息，带有节点信息
func (rl *RaftLogger) Warn(format string, args ...interface{}) {
	rl.BLogger.Warn(rl.getNodeMessage(format), args...)
}

// Error 打印错误信息，带有节点信息
func (rl *RaftLogger) Error(format string, args ...interface{}) {
	rl.BLogger.Error(rl.getNodeMessage(format), args...)
}

func (rl *RaftLogger) Critical(format string, args ...interface{}) {
	rl.BLogger.Critical(rl.getNodeMessage(format), args...)
}

func (rl *RaftLogger) Fatal(err error) {
	rl.BLogger.Fatalf(rl.getNodeMessage(err.Error()))
}

func (rl *RaftLogger) Fatalf(format string, args ...interface{}) {
	rl.BLogger.Fatalf(rl.getNodeMessage(format), args...)
}

func (rl *RaftLogger) SetLogLevel(level LogLevel) {
	rl.BLogger.SetLogLevel(level)
}

func (rl *RaftLogger) getNodeMessage(format string) string {
	return fmt.Sprintf("[Endpoint: %s, ID: %d] %s", rl.endpoint, rl.id, format)
}
