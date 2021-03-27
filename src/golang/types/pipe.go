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
  GetErr() error
}

type PipeWriteEnd interface {
  io.WriteCloser
  Fd() uintptr
  PutErr(err error)
}

// Encapsulates both sides of a pipe.
type Pipe interface {
  io.Closer
  ReadEnd() PipeReadEnd
  WriteEnd() PipeWriteEnd
}

// Dummy type to implement both pipe ends with a standard os.File
type pipeFile struct {
  *os.File
  Common *MockPipe
}
func (file *pipeFile) PutErr(err error) { file.Common.Err = err }
func (file *pipeFile) GetErr() error { return file.Common.Err }

type MockPipe struct {
  read_end  *pipeFile
  write_end *pipeFile
  Err error
}
func NewMockPipe() *MockPipe {
  read_end, write_end, err := os.Pipe()
  pipe := &MockPipe{}
  if err != nil { panic(fmt.Sprintf("failed os.Pipe %v", err)) }
  pipe.read_end = &pipeFile{read_end,pipe}
  pipe.write_end = &pipeFile{write_end,pipe}
  return pipe
}
func (self *MockPipe) Close() error {
  if self.read_end.File != nil { self.read_end.Close() }
  if self.write_end.File != nil { self.write_end.Close() }
  return nil
}
func (self *MockPipe) ReadEnd() PipeReadEnd { return self.read_end }
func (self *MockPipe) WriteEnd() PipeWriteEnd { return self.write_end }


type MockPreloadedPipe struct { MockPipe }
func NewMockPreloadedPipe(data []byte) *MockPreloadedPipe {
  pipe := &MockPreloadedPipe{*NewMockPipe()}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

  done := make(chan bool, 1)
  go func() {
    defer close(done)
    defer pipe.write_end.Close()
    cnt, err := pipe.write_end.Write(data)
    if err != nil || cnt != len(data) { panic(fmt.Sprintf("failed to write %v", err)) }
    done <- true
  }()

  select {
    case <-done:
    case <-ctx.Done(): panic("Write to pipe took too long")
  }
  return pipe
}


type MockDiscardPipe struct { MockPipe }
func NewMockDicardPipe(ctx context.Context) *MockDiscardPipe {
  pipe := &MockDiscardPipe{*NewMockPipe()}

  done := make(chan bool)
  go func() {
    defer close(done)
    _, err := io.Copy(ioutil.Discard, pipe.read_end)
    if err != nil { panic(fmt.Sprintf("failed to discard %v", err)) }
    done <- true
  }()
  go func() {
    select {
      case <-done:
      case <-ctx.Done(): pipe.read_end.Close()
    }
  }()
  return pipe
}


type MockCollectPipe struct {
  MockPipe
  Output <-chan []byte
}
func NewMockCollectPipe(ctx context.Context) *MockCollectPipe {
  done := make(chan []byte, 1)
  pipe := &MockCollectPipe{*NewMockPipe(), done}

  go func() {
    defer close(done)
    buf, err := ioutil.ReadAll(pipe.read_end)
    if err != nil { panic(fmt.Sprintf("failed to collect %v", err)) }
    done <- buf
  }()
  go func() {
    select { case <-ctx.Done(): pipe.read_end.Close() }
  }()
  return pipe
}

