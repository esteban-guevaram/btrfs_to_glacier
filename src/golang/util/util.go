package util

import (
  "bytes"
  "compress/gzip"
  "context"
  "errors"
  "fmt"
  "io"
  "io/fs"
  "os"
  "os/exec"
  fpmod "path/filepath"
  "reflect"
  "unicode"
  "unicode/utf8"

  "btrfs_to_glacier/types"

  "google.golang.org/protobuf/proto"
  "golang.org/x/sys/unix"
)

type PipeImpl struct {
  read_end  io.ReadCloser
  write_end io.WriteCloser
}

func NewInMemPipe(ctx context.Context) *PipeImpl {
  read_end, write_end := io.Pipe()
  pipe := &PipeImpl{
    read_end: read_end,
    write_end: write_end,
  }
  go func() {
    select {
      case <-ctx.Done():
        //Infof("pipe ctx closing")
        read_end.Close()
        write_end.Close()
    }
  }()
  return pipe
}

func NewFileBasedPipe(ctx context.Context) *PipeImpl {
  read_end, write_end, err := os.Pipe()
  if err != nil { Fatalf("failed os.Pipe %v", err) }
  pipe := &PipeImpl{
    read_end: read_end,
    write_end: write_end,
  }
  // Looking at posix pipes, it is not clear whether closing like this is bullet proof
  // see https://man7.org/linux/man-pages/man2/close.2.html
  go func() {
    select {
      case <-ctx.Done():
        //Infof("pipe ctx closing")
        read_end.Close()
        write_end.Close()
    }
  }()
  return pipe
}

func (self *PipeImpl) ReadEnd()  io.ReadCloser { return self.read_end }
func (self *PipeImpl) WriteEnd() io.WriteCloser { return self.write_end }


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
  pipe := NewFileBasedPipe(ctx)
  defer func() { OnlyClosePipeWhenError(pipe, err) }()

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
    defer func() { ClosePipeWithError(pipe, err) }()
    err = command.Wait()
    if err != nil && ctx.Err() == nil {
      err = fmt.Errorf("%v failed: %v\nstderr: %s", args, err, buf_err.Bytes())
    }
  }()
  return pipe.ReadEnd(), nil
}

func ClosePipeWithError(pipe types.Pipe, err error) {
  obj := pipe.WriteEnd()
  CloseWithError(obj, err)
}

func CloseWithError(obj io.Closer, err error) {
  if err != nil {
    defer Warnf("CloseWithError: %v", err)
    if adv_obj,ok := obj.(types.CloseWithErrIf); ok {
      adv_obj.CloseWithError(err)
      return
    }
  }
  obj.Close()
}

func OnlyClosePipeWhenError(pipe types.Pipe, err error) {
  if err == nil { return }
  ClosePipeWithError(pipe, err)
}

// Closes the channel if `err != nil`.
// If `channel` is of type `channel error` then attempts to send the error before closing.
func OnlyCloseChanWhenError(channel interface{}, err error) {
  if err == nil { return }
  rv := reflect.ValueOf(channel)
  if rk := rv.Kind(); rk != reflect.Chan {
    Fatalf("expecting type: 'chan ...'  instead got: %s", rk.String())
  }
  is_chan_err := rv.Type().Elem() == reflect.TypeOf((*error)(nil)).Elem()
  is_send_dir := rv.Type().ChanDir() != reflect.RecvDir
  if is_chan_err && is_send_dir {
    if ok := rv.TrySend(reflect.ValueOf(err)); !ok { Fatalf("failed to send before closing: %v", err) }
  }
  rv.Close()
}

func OnlyCloseWhenError(obj io.Closer, err error) {
  if err == nil { return }
  CloseWithError(obj, err)
}

func WrapInChan(err error) (<-chan error) {
  done := make(chan error, 1)
  done <- err
  close(done)
  return done
}

func PbErrorf(format string, pbs ...proto.Message) error {
  return errors.New(PbPrintf(format, pbs...))
}

func IsOnlyAsciiString(str string, allow_ctrl bool) error {
  if !utf8.ValidString(str) { return fmt.Errorf("String is not valid unicode") }
  for idx,codept := range str {
    // only ascii non control are allowed
    if codept > unicode.MaxASCII || (!allow_ctrl && unicode.IsControl(codept)) {
      return fmt.Errorf("String is has invalid characters at %d: '0x%x'", idx, codept)
    }
  }
  return nil
}

func IsDir(path string) bool {
  f_info, err := os.Lstat(path)
  if err != nil { return false }
  return f_info.IsDir()
}

func IsSymLink(path string) bool {
  f_info, err := os.Lstat(path)
  if err != nil { return false }
  return f_info.Mode() & fs.ModeSymlink != 0
}

// os.IsNotExists does not do sh*t
func IsNotExist(err error) bool {
  return os.IsNotExist(err) || errors.Is(err, unix.ENOENT)
}

func Exists(path string) bool {
  _, err := os.Lstat(path)
  return !IsNotExist(err)
}

func UnmarshalGzProto(path string, msg proto.Message) error {
  f, err := os.Open(path)
  if err != nil { return err }
  defer f.Close()

  reader, err := gzip.NewReader(f)
  if err != nil { return err }
  defer reader.Close()

  data, err := io.ReadAll(reader)
  if err != nil { return err }
  err = proto.Unmarshal(data, msg)
  return err
}

func MarshalGzProto(path string, msg proto.Message) error {
  f, err := os.Create(path)
  if err != nil { return err }
  defer f.Close()

  data, err := proto.Marshal(msg)
  if err != nil { return err }

  writer := gzip.NewWriter(f)
  _,err = writer.Write(data)
  if err != nil { writer.Close(); return err }
  return writer.Close()
}

func RemoveAll(path string) error {
  tmpdir := os.TempDir()
  if !fpmod.HasPrefix(path, tmpdir) {
    return fmt.Errorf("HasPrefix('%s', '%s')", path, tmpdir)
  }
  return os.RemoveAll(path)
}

