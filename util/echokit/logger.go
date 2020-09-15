package echokit

import (
	"io"
	"github.com/jajotz/utilities-golang/logs"

	"github.com/labstack/gommon/log"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type LoggerWrapper struct {
	log    *logrus.Logger
	prefix string
	level  log.Lvl
}

func (l *LoggerWrapper) Output() io.Writer {
	return l.log.Out
}

func (l *LoggerWrapper) SetOutput(w io.Writer) {
	l.log.Out = w
}

func (l *LoggerWrapper) Prefix() string {
	return l.prefix
}

func (l *LoggerWrapper) SetPrefix(prefix string) {
	l.prefix = prefix
}

func (l *LoggerWrapper) Level() log.Lvl {
	return l.level
}

func (l *LoggerWrapper) SetLevel(v log.Lvl) {
	l.level = v
}

func (l *LoggerWrapper) SetHeader(header string) {
}

func (l *LoggerWrapper) Print(i ...interface{}) {
	l.log.Print(i)
}

func (l *LoggerWrapper) Printj(j log.JSON) {
	l.log.Printf("%v\n", j)
}

func (l *LoggerWrapper) Debugf(format string, i ...interface{}) {
	l.log.Debug(format, i)
}

func (l *LoggerWrapper) Debug(i ...interface{}) {
	l.log.Debug(i)
}

func (l *LoggerWrapper) Debugj(j log.JSON) {
	l.log.Debugf("%v\n", j)
}

func (l *LoggerWrapper) Infof(format string, i ...interface{}) {
	l.log.Infof(format, i)
}

func (l *LoggerWrapper) Info(i ...interface{}) {
	l.log.Info(i)
}

func (l *LoggerWrapper) Infoj(j log.JSON) {
	l.log.Infof("%v\n", j)
}

func (l *LoggerWrapper) Printf(format string, i ...interface{}) {
	l.log.Printf(format, i)
}

func (l *LoggerWrapper) Warn(i ...interface{}) {
	l.log.Warn(i)
}

func (l *LoggerWrapper) Warnj(j log.JSON) {
	l.log.Warnf("%v\n", j)
}

func (l *LoggerWrapper) Warnf(format string, i ...interface{}) {
	l.log.Warnf(format, i)
}

func (l *LoggerWrapper) Error(i ...interface{}) {
	l.log.Error(i)
}

func (l *LoggerWrapper) Errorj(j log.JSON) {
	l.log.Errorf("%v\n", j)
}

func (l *LoggerWrapper) Errorf(format string, i ...interface{}) {
	l.log.Errorf(format, i)
}

func (l *LoggerWrapper) Fatal(i ...interface{}) {
	l.log.Fatal(i)
}

func (l *LoggerWrapper) Fatalj(j log.JSON) {
	l.log.Fatalf("%v\n", j)
}

func (l *LoggerWrapper) Fatalf(format string, i ...interface{}) {
	l.log.Fatalf(format, i)
}

func (l *LoggerWrapper) Panic(i ...interface{}) {
	l.log.Panic(i)
}

func (l *LoggerWrapper) Panicj(j log.JSON) {
	l.log.Panicf("%v\n", j)
}

func (l *LoggerWrapper) Panicf(format string, i ...interface{}) {
	l.log.Panicf(format, i)
}

func NewLoggerWrapper(logger logs.Logger) (*LoggerWrapper, error) {
	if instance, ok := logger.Instance().(*logrus.Logger); ok {
		return &LoggerWrapper{log: instance}, nil
	}

	return nil, errors.New("logger is not type of logs.Logger")
}
