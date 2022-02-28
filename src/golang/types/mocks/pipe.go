package mocks

import (
  "context"
  "errors"
  "io"
  "time"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"
)

var ErrIoPipe = errors.New("pipe_err_io")

type ErrorIo struct {
  IoErr error
  CloseErr error
}
func (self *ErrorIo) Read(p []byte) (n int, err error) { return 0, self.IoErr }
func (self *ErrorIo) Write(p []byte) (n int, err error) { return 0, self.IoErr }
func (self *ErrorIo) Close() error { return self.CloseErr }
func (self *ErrorIo) ReadEnd()  types.ReadEndIf  { return self }
func (self *ErrorIo) WriteEnd() types.WriteEndIf { return self }
func (self *ErrorIo) GetErr() error { return self.IoErr }
func (self *ErrorIo) SetErr(err error) {}
func (self *ErrorIo) CloseWithError(err error) error { self.CloseErr = err; return err }

func NewErrorPipe() types.Pipe {
  pipe := &ErrorIo{ IoErr: ErrIoPipe }
  return pipe
}

func NewPreloadedPipe(data []byte) types.Pipe {
  pipe := util.NewFileBasedPipe(context.TODO())
  done := make(chan bool, 1)

  go func() {
    defer close(done)
    defer pipe.WriteEnd().Close()
    cnt, err := pipe.WriteEnd().Write(data)
    if err != nil || cnt != len(data) { util.Fatalf("failed to write %v", err) }
    done <- true
  }()

  select {
    case <-done:
    case <-time.After(1 * time.Millisecond):
      util.Fatalf("write to pipe took too long")
  }
  return pipe
}
// Use when there is too much data for the pipe buffer
func NewBigPreloadedPipe(ctx context.Context, data []byte) types.Pipe {
  pipe := util.NewFileBasedPipe(ctx)
  done := make(chan bool, 1)

  go func() {
    defer close(done)
    cnt, err := pipe.WriteEnd().Write(data)
    if err != nil || cnt != len(data) { util.Fatalf("failed to write %v", err) }
    done <- true
  }()
  go func() {
    defer pipe.WriteEnd().Close()
    select {
      case <-done:
      case <-ctx.Done():
    }
  }()
  return pipe
}


type DiscardPipe struct { types.Pipe }
func NewMockDicardPipe(ctx context.Context) *DiscardPipe {
  pipe := &DiscardPipe{util.NewFileBasedPipe(context.TODO())}

  done := make(chan bool)
  go func() {
    defer close(done)
    _, err := io.Copy(io.Discard, pipe.ReadEnd())
    if err != nil { util.Fatalf("failed to discard %v", err) }
    done <- true
  }()
  go func() {
    defer pipe.ReadEnd().Close()
    select {
      case <-done:
      case <-ctx.Done():
    }
  }()
  return pipe
}


type CollectPipe struct {
  types.Pipe
  Output <-chan []byte
}
func NewCollectPipe(ctx context.Context) *CollectPipe {
  done := make(chan []byte, 1)
  pipe := &CollectPipe{util.NewFileBasedPipe(ctx), done}

  go func() {
    defer close(done)
    buf, err := io.ReadAll(pipe.ReadEnd())
    if err != nil { util.Fatalf("failed to collect %v", err) }
    done <- buf
  }()
  go func() {
    select { case <-ctx.Done(): pipe.ReadEnd().Close() }
  }()
  return pipe
}


