package utils

import "fmt"

type Logger interface {
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

type SimpleLogger struct{}

func NewSimpleLogger() *SimpleLogger {
	return &SimpleLogger{}
}

func (l *SimpleLogger) Info(msg string)  { fmt.Println("INFO: " + msg) }
func (l *SimpleLogger) Warn(msg string)  { fmt.Println("WARN: " + msg) }
func (l *SimpleLogger) Error(msg string) { fmt.Println("ERROR: " + msg) }
