package types

import "context"
import "fmt"
import "io"
import "io/ioutil"
import "os"
import "time"

type PipeReadEnd interface {
  io.ReadCloser
  Fd() uintptr
}

type PipeWriteEnd interface {
  io.WriteCloser
  Fd() uintptr
}

// Encapsulates both sides of a pipe.
type Pipe interface {
  io.Closer
  ReadEnd() PipeReadEnd
  WriteEnd() PipeWriteEnd
}

type MockPreloadedPipe struct {
  read_end *os.File
  write_end *os.File
}

func NewMockPreloadedPipe(data []byte) *MockPreloadedPipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { panic(fmt.Sprintf("failed os.Pipe %v", err)) }

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  done := make(chan bool, 1)
  go func() {
    defer close(done)
    defer write_end.Close()
    cnt, err := write_end.Write(data)
    if err != nil || cnt != len(data) { panic(fmt.Sprintf("failed to write %v", err)) }
    done <- true
  }()

  select {
    case <-done:
    case <-ctx.Done(): panic("Write to pipe took too long")
  }
  return &MockPreloadedPipe{read_end, write_end}
}

func (self *MockPreloadedPipe) Close() error {
  if self.read_end != nil { self.read_end.Close() }
  if self.write_end != nil { self.write_end.Close() }
  return nil
}
func (self *MockPreloadedPipe) ReadEnd() PipeReadEnd { return self.read_end }
func (self *MockPreloadedPipe) WriteEnd() PipeWriteEnd { return nil }

type MockDiscardPipe struct {
  read_end *os.File
  write_end *os.File
}

func (self *MockDiscardPipe) Close() error {
  if self.read_end != nil { self.read_end.Close() }
  if self.write_end != nil { self.write_end.Close() }
  return nil
}
func (self *MockDiscardPipe) ReadEnd() PipeReadEnd { return nil }
func (self *MockDiscardPipe) WriteEnd() PipeWriteEnd { return self.write_end }

func NewMockDicardPipe(ctx context.Context) *MockDiscardPipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { panic(fmt.Sprintf("failed os.Pipe %v", err)) }

  done := make(chan bool)
  go func() {
    defer close(done)
    _, err := io.Copy(ioutil.Discard, read_end)
    if err != nil { panic(fmt.Sprintf("failed to discard %v", err)) }
    done <- true
  }()
  go func() {
    select {
      case <-done:
      case <-ctx.Done(): read_end.Close()
    }
  }()
  return &MockDiscardPipe{read_end, write_end}
}

type MockCollectPipe struct {
  MockDiscardPipe
  Output <-chan []byte
}

func NewMockCollectPipe(ctx context.Context) *MockCollectPipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { panic(fmt.Sprintf("failed os.Pipe %v", err)) }

  done := make(chan []byte, 1)
  pipe := &MockCollectPipe{MockDiscardPipe{read_end, write_end}, done}
  go func() {
    defer close(done)
    buf, err := ioutil.ReadAll(read_end)
    if err != nil { panic(fmt.Sprintf("failed to collect %v", err)) }
    done <- buf
  }()
  go func() {
    select { case <-ctx.Done(): read_end.Close() }
  }()
  return pipe
}

