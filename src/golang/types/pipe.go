package types

import "context"
import "fmt"
import "io"
import "io/ioutil"
import "os"
import "time"

type HasFileDescriptorIf interface { Fd() uintptr }
type CloseWithErrIf interface { CloseWithError(err error) error }

// Encapsulates both sides of a pipe.
type Pipe interface {
  ReadEnd() io.ReadCloser
  WriteEnd() io.WriteCloser
}

// Dummy type to implement both pipe ends with a standard os.File
type pipeFile struct {
  *os.File
  Common *MockPipe
}
func (file *pipeFile) CloseWithError(err error) error {
  if file.Common.Err == nil { file.Common.Err = err }
  file.Close()
  return nil
}
func (file *pipeFile) Read(data []byte) (n int, err error) {
  if file.Common.Err != nil { return 0, file.Common.Err }
  return file.File.Read(data)
}
func (file *pipeFile) ReadFrom(inner io.Reader) (n int64, err error) {
  if file.Common.Err != nil { return 0, file.Common.Err }
  return file.File.ReadFrom(inner)
}

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
func (self *MockPipe) ReadEnd() io.ReadCloser { return self.read_end }
func (self *MockPipe) WriteEnd() io.WriteCloser { return self.write_end }


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
// Use when there is too much data for the pipe buffer
func NewMockBigPreloadedPipe(ctx context.Context, data []byte) *MockPreloadedPipe {
  pipe := &MockPreloadedPipe{*NewMockPipe()}

  done := make(chan bool, 1)
  go func() {
    defer close(done)
    cnt, err := pipe.write_end.Write(data)
    if err != nil || cnt != len(data) { panic(fmt.Sprintf("failed to write %v", err)) }
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

