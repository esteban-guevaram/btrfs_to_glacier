package mocks

import (
  "fmt"
  "reflect"
  "regexp"
  "runtime"
)

type InjectT = func(interface{}) error
type ErrBase struct {
  ErrInject func(interface{}) error
}

var name_rx *regexp.Regexp
func init() {
  name_rx = regexp.MustCompile(`.*\.(\w+)-?.*$`)
}

func MethodName(m interface{}) string {
  v := reflect.ValueOf(m)
  p := v.Pointer()
  f := runtime.FuncForPC(p)
  mangled := f.Name()
  return name_rx.FindStringSubmatch(mangled)[1]
}

func MethodMatch(m1 interface{}, m2 interface{}) bool {
  return MethodName(m1) == MethodName(m2)
}

func (self *ErrBase) SetErrInject(f InjectT) {
  self.ErrInject = f
}

func (self *ErrBase) ForAllErr(err error) {
  self.ErrInject = func(interface{}) error { return err }
}

func (self *ErrBase) ForAllErrMsg(msg string) {
  self.ErrInject = func(interface{}) error { return fmt.Errorf(msg) }
}

func (self *ErrBase) ForMethodErrMsg(method interface{}, msg string) {
  self.ErrInject = func(called interface{}) error {
    if !MethodMatch(method, called) { return nil }
    return fmt.Errorf(msg)
  }
}

