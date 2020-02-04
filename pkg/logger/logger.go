package logger

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

// levels
const (
	debugLevel = 0
	infoLevel  = 1
	warnLevel  = 2
	errorLevel = 3
	fatalLevel = 4
)

const (
	printDebugLevel = "[debug  ] "
	printInfoLevel  = "[info   ] "
	printWarnLevel  = "[warn   ]"
	printErrorLevel = "[error  ] "
	printFatalLevel = "[fatal  ] "
)

var (
	Logger *logger
)

type logger struct {
	level      int
	baseLogger *log.Logger
	baseFile   *os.File
	url        string
}

func New(url,strLevel string, pathname string, flag int) error {
	// level
	var level int
	switch strings.ToLower(strLevel) {
	case "debug":
		level = debugLevel
	case "info":
		level = infoLevel
	case "warn":
		level = warnLevel
	case "error":
		level = errorLevel
	case "fatal":
		level = fatalLevel
	default:
		return errors.New("unknown level: " + strLevel)
	}

	// logger
	var baseLogger *log.Logger
	var baseFile *os.File
	if pathname != "" {
		now := time.Now()

		filename := fmt.Sprintf("%d%02d%02d_%02d_%02d_%02d.log",
			now.Year(),
			now.Month(),
			now.Day(),
			now.Hour(),
			now.Minute(),
			now.Second())

		file, err := os.Create(path.Join(pathname, filename))
		if err != nil {
			return  err
		}

		baseLogger = log.New(file, "", flag)
		baseFile = file
	} else {
		baseLogger = log.New(os.Stdout, "", flag)
	}

	// new
	Logger = new(logger)
	Logger.level = level
	Logger.baseLogger = baseLogger
	Logger.baseFile = baseFile
	Logger.url = url

	return  nil
}

// It's dangerous to call the method on logging
func (logger *logger) Close() {
	if logger.baseFile != nil {
		logger.baseFile.Close()
	}

	logger.baseLogger = nil
	logger.baseFile = nil
}

func (logger *logger) doPrintf(level int, printLevel string, format string, a ...interface{}) {
	if level < logger.level {
		return
	}
	if logger.baseLogger == nil {
		panic("logger closed")
	}
	format = printLevel + format
	args := fmt.Sprintf(format, a...)

	logger.baseLogger.Output(3,args)

	if level == fatalLevel {
		os.Exit(1)
	}
}

func (logger *logger) Debug(format string, a ...interface{}) {
	logger.doPrintf(debugLevel, printDebugLevel, format, a...)
}

func (logger *logger) Info(format string, a ...interface{}) {
	logger.doPrintf(infoLevel, printInfoLevel, format, a...)
}

func (logger *logger) Warn(format string, a ...interface{}) {
	logger.doPrintf(warnLevel, printWarnLevel, format, a...)
}

func (logger *logger) Error(format string, a ...interface{}) {
	// todo 待优化 add pool and use client Do
	if len(logger.url) != 0 {
		args := fmt.Sprintf(format, a...)
		http.Post(logger.url, "application/json", strings.NewReader(args))
	}
	logger.doPrintf(errorLevel, printErrorLevel, format, a...)
}

func (logger *logger) Fatal(format string, a ...interface{}) {
	logger.doPrintf(fatalLevel, printFatalLevel, format, a...)
}
