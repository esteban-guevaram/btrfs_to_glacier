package util

import "bytes"
import "context"
import "fmt"
import "io"
import "os"
import "os/exec"
import "btrfs_to_glacier/types"

type FileBasedPipe struct {
  read_end  *os.File
  write_end *os.File
}

func NewFileBasedPipe() *FileBasedPipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { Fatalf("failed os.Pipe %v", err) }
  pipe := &FileBasedPipe{ }
  pipe.read_end =  read_end
  pipe.write_end = write_end
  return pipe
}

func (self *FileBasedPipe) ReadEnd()  io.ReadCloser { return self.read_end }
func (self *FileBasedPipe) WriteEnd() io.WriteCloser { return self.write_end }


// Synchronous, waits for the command to finish
// Takes ownership of `input` and will close it once done.
func StartCmdWithPipedInput(ctx context.Context, input io.ReadCloser, args []string) error {
  var err error
  buf_err := new(bytes.Buffer)
  buf_out := new(bytes.Buffer)
  defer input.Close()

  command := exec.CommandContext(ctx, args[0], args[1:]...)
  command.Stdin = input
  command.Stdout = buf_out
  command.Stderr = buf_err

  err = command.Start()
  if err != nil {
    return fmt.Errorf("%v: %v", args, err)
  }
  Infof("%v started as pid %d", args, command.Process.Pid)

  err = command.Wait()
  if err != nil && ctx.Err() == nil {
    return fmt.Errorf("%v failed: %v\nstderr: %s", args, err, buf_err.Bytes())
  }
  Infof("%v done, output:\n%s", args, buf_out)
  return nil
}

func StartCmdWithPipedOutput(ctx context.Context, args []string) (io.ReadCloser, error) {
  var err error
  pipe := NewFileBasedPipe()
  defer func() { OnlyCloseWhenError(pipe, err) }()

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
    var err error
    defer func() { CloseWithError(pipe, err) }()
    err = command.Wait()
    if err != nil && ctx.Err() == nil {
      err = fmt.Errorf("%v failed: %v\nstderr: %s", args, err, buf_err.Bytes())
    }
  }()
  return pipe.ReadEnd(), nil
}

func CloseWithError(pipe types.Pipe, err error) {
  obj := pipe.WriteEnd()
  if err != nil {
    if adv_obj,ok := obj.(types.CloseWithErrIf); ok {
      adv_obj.CloseWithError(err)
      return
    }
  }
  obj.Close()
}

func OnlyCloseWhenError(pipe types.Pipe, err error) {
  if err == nil { return }
  CloseWithError(pipe, err)
}

