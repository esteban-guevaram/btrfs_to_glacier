package util

import "log"

func Fatalf(format string, v ...interface{}) {
  log.Fatalf(format, v...)
}

func Infof(format string, v ...interface{}) {
  log.Printf(format, v...)
}

