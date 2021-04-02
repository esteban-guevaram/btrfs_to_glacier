package util

import "bytes"
import "context"
import "fmt"
import "io"
import "os"
import "os/exec"
import "sync"
import "btrfs_to_glacier/types"

type pipeFile struct {
  *os.File
  pipe *FileBasedPipe
}

type FileBasedPipe struct {
  read_end  *pipeFile
  write_end *pipeFile
  err_mu sync.Mutex
  err    error
}

func NewFileBasedPipe() *FileBasedPipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { panic(fmt.Sprintf("failed os.Pipe %v", err)) }
  pipe := &FileBasedPipe{ }
  pipe.read_end =  &pipeFile{read_end, pipe}
  pipe.write_end = &pipeFile{write_end, pipe}
  return pipe
}

func (self *FileBasedPipe) Close() error {
  if self.read_end != nil { self.read_end.Close() }
  if self.write_end != nil { self.write_end.Close() }
  return nil
}
func (self *FileBasedPipe) ReadEnd()  *pipeFile { return self.read_end }
func (self *FileBasedPipe) WriteEnd() *pipeFile { return self.write_end }

func (self *pipeFile) PutErr(err error) {
  self.pipe.err_mu.Lock()
  defer self.pipe.err_mu.Unlock()
  self.pipe.err = err
}
func (self *pipeFile) GetErr() error {
  self.pipe.err_mu.Lock()
  defer self.pipe.err_mu.Unlock()
  return self.pipe.err
}


// Synchronous, waits for the command to finish
// Takes ownership of `input` and will close it once done.
func StartCmdWithPipedInput(ctx context.Context, input types.PipeReadEnd, args []string) error {
  var err error
  buf_err := new(bytes.Buffer)
  buf_out := new(bytes.Buffer)
  defer input.Close()

  command := exec.CommandContext(ctx, args[0], args[1:]...)
  command.Stdout = buf_out
  command.Stderr = buf_err
  switch pipe_impl := input.(type) {
    case *pipeFile: command.Stdin = pipe_impl.File
    default: command.Stdin = pipe_impl
  }

  err = command.Start()
  if err != nil {
    return fmt.Errorf("%v: %v", args, err)
  }
  Infof("%v started as pid %d", args, command.Process.Pid)

  err = command.Wait()
  if input.GetErr() != nil {
    return fmt.Errorf("Input pipe had error: %s", input.GetErr())
  }
  if err != nil && ctx.Err() == nil {
    return fmt.Errorf("%v failed: %v\nstderr: %s", args, err, buf_err.Bytes())
  }
  Infof("%v done, output:\n%s", args, buf_out)
  return nil
}

func StartCmdWithPipedOutput(ctx context.Context, args []string) (types.PipeReadEnd, error) {
  var err error
  pipe := NewFileBasedPipe()
  defer CloseIfProblemo(pipe, &err)

  buf_err := new(bytes.Buffer)
  command := exec.CommandContext(ctx, args[0], args[1:]...)
  command.Stdout = pipe.WriteEnd()
  command.Stderr = buf_err

  err = command.Start()
  if err != nil {
    return nil, fmt.Errorf("%v: %v", args, err)
  }
  Infof("%v started as pid %d", args, command.Process.Pid)

  go func() {
    defer pipe.WriteEnd().Close()
    err := command.Wait()
    if err != nil && ctx.Err() == nil {
      err = fmt.Errorf("%v failed: %v\nstderr: %s", args, err, buf_err.Bytes())
    }
    pipe.WriteEnd().PutErr(err) // do this before closing stream
  }()
  return pipe.ReadEnd(), nil
}

func CloseIfProblemo(obj io.Closer, err_ptr *error) {
  if *err_ptr != nil { obj.Close() }
}

