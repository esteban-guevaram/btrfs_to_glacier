package mocks

import (
  "context"
  "errors"
  "io"
  "os"
  "time"

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

type Pipe struct {
  read_end  io.ReadCloser
  write_end io.WriteCloser
}
func NewPipe() *Pipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { util.Fatalf("failed os.Pipe %v", err) }
  pipe := &Pipe{
    read_end:  read_end,
    write_end: write_end,
  }
  return pipe
}
func NewErrorPipe() *Pipe {
  end := &ErrorIo{ IoErr: ErrIoPipe }
  pipe := &Pipe{
    read_end:  end,
    write_end: end,
  }
  return pipe
}
func (self *Pipe) ReadEnd() io.ReadCloser { return self.read_end }
func (self *Pipe) WriteEnd() io.WriteCloser { return self.write_end }

func NewPreloadedPipe(data []byte) *Pipe {
  pipe := NewPipe()
  done := make(chan bool, 1)

  go func() {
    defer close(done)
    defer pipe.write_end.Close()
    cnt, err := pipe.write_end.Write(data)
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
func NewBigPreloadedPipe(ctx context.Context, data []byte) *Pipe {
  pipe := NewPipe()
  done := make(chan bool, 1)

  go func() {
    defer close(done)
    cnt, err := pipe.write_end.Write(data)
    if err != nil || cnt != len(data) { util.Fatalf("failed to write %v", err) }
    done <- true
  }()
  go func() {
    defer pipe.write_end.Close()
    select {
      case <-done:
      case <-ctx.Done():
    }
  }()
  return pipe
}


type DiscardPipe struct { Pipe }
func NewMockDicardPipe(ctx context.Context) *DiscardPipe {
  pipe := &DiscardPipe{*NewPipe()}

  done := make(chan bool)
  go func() {
    defer close(done)
    _, err := io.Copy(io.Discard, pipe.read_end)
    if err != nil { util.Fatalf("failed to discard %v", err) }
    done <- true
  }()
  go func() {
    defer pipe.read_end.Close()
    select {
      case <-done:
      case <-ctx.Done():
    }
  }()
  return pipe
}


type CollectPipe struct {
  Pipe
  Output <-chan []byte
}
func NewCollectPipe(ctx context.Context) *CollectPipe {
  done := make(chan []byte, 1)
  pipe := &CollectPipe{*NewPipe(), done}

  go func() {
    defer close(done)
    buf, err := io.ReadAll(pipe.read_end)
    if err != nil { util.Fatalf("failed to collect %v", err) }
    done <- buf
  }()
  go func() {
    select { case <-ctx.Done(): pipe.read_end.Close() }
  }()
  return pipe
}


