package mocks

import (
  "bytes"
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
  pipe := util.NewFileBasedPipe(context.Background())
  done := make(chan bool)
  defer close(done)
  defer pipe.WriteEnd().Close()

  go func() {
    select {
      case <-done:
      case <-time.After(1 * time.Millisecond):
        util.Fatalf("write to pipe took too long")
    }
  }()

  cnt, err := pipe.WriteEnd().Write(data)
  if err != nil || cnt != len(data) { util.Fatalf("failed to write %v", err) }
  return pipe
}
// Use when there is too much data for the pipe buffer
func NewBigPreloadedPipe(ctx context.Context, data []byte) types.Pipe {
  pipe := util.NewFileBasedPipe(ctx)
  done := make(chan bool)

  go func() {
    defer close(done)
    defer pipe.WriteEnd().Close()
    cnt, err := pipe.WriteEnd().Write(data)
    if err != nil || cnt != len(data) { util.Fatalf("failed to write %v", err) }
  }()
  go func() {
    select {
      case <-done:
      case <-ctx.Done():
        util.Warnf("NewBigPreloadedPipe Timeout while writing %d bytes", len(data))
    }
  }()
  return pipe
}


type CollectPipe struct {
  types.Pipe
  Discard bool
  Done chan bool
  Result []byte
}

func (self *CollectPipe) BlockingResult(ctx context.Context) []byte {
  select {
    case <-self.Done: return self.Result
    case <-ctx.Done():
      util.Warnf("CollectPipe.BlockingResult Timeout")
      return nil
  }
}

func (self *CollectPipe) StartCollectingAsync(ctx context.Context) {
  if self.Done != nil { util.Fatalf("already started") }
  self.Done = make(chan bool)
  go func() {
    select {
      case <-self.Done:
      case <-ctx.Done():
        util.Warnf("CollectPipe.StartCollectingAsync Timeout")
    }
  }()
  go func() {
    defer close(self.Done)
    defer self.ReadEnd().Close()
    if self.Discard {
      _, err := io.Copy(io.Discard, self.ReadEnd())
      if err != nil { util.Fatalf("CollectPipe.StartCollectingAsync: %v", err) }
    }
    /*else*/ if !self.Discard {
      sink := new(bytes.Buffer)
      _, err := io.Copy(sink, self.ReadEnd())
      if err != nil { util.Fatalf("CollectPipe.StartCollectingAsync: %v", err) }
      self.Result = sink.Bytes()
    }
  }()
}

func NewMockDicardPipe(ctx context.Context) *CollectPipe {
  pipe := &CollectPipe{
    Pipe:util.NewFileBasedPipe(ctx),
    Discard: true, }
  pipe.StartCollectingAsync(ctx)
  return pipe
}

