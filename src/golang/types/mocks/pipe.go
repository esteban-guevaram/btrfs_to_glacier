package mocks

import (
  "context"
  "io"
  "os"
  "time"

  "btrfs_to_glacier/util"
)

// Dummy type to implement both pipe ends with a standard os.File
type pipeFile struct {
  *os.File
  Common *Pipe
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

type Pipe struct {
  read_end  *pipeFile
  write_end *pipeFile
  Err error
}
func NewPipe() *Pipe {
  read_end, write_end, err := os.Pipe()
  pipe := &Pipe{}
  if err != nil { util.Fatalf("failed os.Pipe %v", err) }
  pipe.read_end = &pipeFile{read_end,pipe}
  pipe.write_end = &pipeFile{write_end,pipe}
  return pipe
}
func (self *Pipe) ReadEnd() io.ReadCloser { return self.read_end }
func (self *Pipe) WriteEnd() io.WriteCloser { return self.write_end }


type PreloadedPipe struct { Pipe }
func NewPreloadedPipe(data []byte) *PreloadedPipe {
  pipe := &PreloadedPipe{*NewPipe()}

  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()

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
    case <-ctx.Done(): util.Fatalf("write to pipe took too long")
  }
  return pipe
}
// Use when there is too much data for the pipe buffer
func NewBigPreloadedPipe(ctx context.Context, data []byte) *PreloadedPipe {
  pipe := &PreloadedPipe{*NewPipe()}

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


