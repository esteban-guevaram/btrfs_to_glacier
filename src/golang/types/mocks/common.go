package mocks

import (
  "fmt"
)

type InjectT = func(string) error
type ErrBase struct {
  ErrInject InjectT
}

func (self *ErrBase) SetErrInject(f InjectT) {
  self.ErrInject = f
}

func (self *ErrBase) ForAllErr(err error) {
  self.ErrInject = func(string) error { return err }
}

func (self *ErrBase) ForAllErrMsg(msg string) {
  self.ErrInject = func(string) error { return fmt.Errorf(msg) }
}

func (self *ErrBase) ForMethodErrMsg(method string, msg string) {
  self.ErrInject = func(called string) error {
    if method != called { return nil }
    return fmt.Errorf(msg)
  }
}

