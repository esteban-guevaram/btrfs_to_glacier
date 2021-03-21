package util

import "bytes"
import "context"
import "fmt"
import "io"
import "os"
import "os/exec"
import "btrfs_to_glacier/types"

type FileBasedPipe struct {
  read_end *os.File
  write_end *os.File
}

func (self *FileBasedPipe) Close() error {
  if self.read_end != nil { self.read_end.Close() }
  if self.write_end != nil { self.write_end.Close() }
  return nil
}
func (self *FileBasedPipe) ReadEnd() *os.File { return self.read_end }
func (self *FileBasedPipe) WriteEnd() *os.File { return self.write_end }

func NewFileBasedPipe() *FileBasedPipe {
  read_end, write_end, err := os.Pipe()
  if err != nil { panic(fmt.Sprintf("failed os.Pipe %v", err)) }
  return &FileBasedPipe{read_end, write_end}
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
      Warnf("%v failed: %v\nstderr: %s", args, err, buf_err.Bytes())
    }
  }()
  return pipe.ReadEnd(), nil
}

func CloseIfProblemo(obj io.Closer, err_ptr *error) {
  if *err_ptr != nil { obj.Close() }
}

