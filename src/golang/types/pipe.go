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
  Close()
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

  done := make(chan bool)
  go func() {
    cnt, err := write_end.Write(data)
    if err != nil || cnt != len(data) { panic(fmt.Sprintf("failed to write %v", err)) }
    write_end.Close()
    done <- true
  }()

  select {
    case <-done:
    case <-ctx.Done(): panic("Write to pipe took too long")
  }
  return &MockPreloadedPipe{read_end, write_end}
}

func (self *MockPreloadedPipe) Close() {
  if self.read_end != nil { self.read_end.Close() }
  if self.write_end != nil { self.write_end.Close() }
}
func (self *MockPreloadedPipe) ReadEnd() PipeReadEnd { return self.read_end }
func (self *MockPreloadedPipe) WriteEnd() PipeWriteEnd { return nil }

type MockDiscardPipe struct {
  read_end *os.File
  write_end *os.File
}

func (self *MockDiscardPipe) Close() {
  if self.read_end != nil { self.read_end.Close() }
  if self.write_end != nil { self.write_end.Close() }
}
func (self *MockDiscardPipe) ReadEnd() PipeReadEnd { return nil }
func (self *MockDiscardPipe) WriteEnd() PipeWriteEnd { return self.write_end }

func NewMockDicardPipe(ctx context.Context) *MockDiscardPipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { panic(fmt.Sprintf("failed os.Pipe %v", err)) }

  done := make(chan bool)
  go func() {
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

