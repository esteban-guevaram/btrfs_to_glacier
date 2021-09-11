package util

import "context"
import "io"
import "testing"
import "time"
import "btrfs_to_glacier/types"

func runCmdGetOutputOrDie(ctx context.Context, t *testing.T, args []string) <-chan []byte {
  read_end, err := StartCmdWithPipedOutput(ctx, args)
  if err != nil {
    t.Fatalf("%v failed: %v", args, err)
  }

  done := make(chan []byte, 1)
  go func() {
    defer read_end.Close()
    data, _ := io.ReadAll(read_end)
    done <- data
  }()

  return done
}

func testInMemPipeCtxCancel_Helper(t *testing.T, pipe types.Pipe, pipe_f func([]byte)) {
  done := make(chan bool, 1)
  go func() {
    defer close(done)
    buf := make([]byte, 1024*1024) //big enough to fill a file pipe
    pipe_f(buf)
  }()
  select {
    case <-done: return
    case <-time.After(17*time.Millisecond):
      t.Fatalf("context timeout was not taken into account")
  }
}

func TestInMemPipeCtxCancel_WhileWriting(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  pipe := NewInMemPipe(ctx)
  pipe_f := func(buf []byte) { pipe.WriteEnd().Write(buf) }
  testInMemPipeCtxCancel_Helper(t, pipe, pipe_f)
}

func TestFileBasedPipeCtxCancel_WhileWriting(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  pipe := NewFileBasedPipe(ctx)
  pipe_f := func(buf []byte) { pipe.WriteEnd().Write(buf) }
  testInMemPipeCtxCancel_Helper(t, pipe, pipe_f)
}

func TestInMemPipeCtxCancel_WhileReading(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  pipe := NewInMemPipe(ctx)
  pipe_f := func(buf []byte) { pipe.ReadEnd().Read(buf) }
  testInMemPipeCtxCancel_Helper(t, pipe, pipe_f)
}

func TestFileBasedPipeCtxCancel_WhileReading(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  pipe := NewFileBasedPipe(ctx)
  pipe_f := func(buf []byte) { pipe.ReadEnd().Read(buf) }
  testInMemPipeCtxCancel_Helper(t, pipe, pipe_f)
}

func TestFileBasedPipeHasGoodImpl(t *testing.T) {
  pipe := NewFileBasedPipe(context.TODO())
  defer pipe.WriteEnd().Close()
  defer pipe.ReadEnd().Close()
  if _,ok := pipe.ReadEnd().(types.HasFileDescriptorIf); !ok { t.Error("bad impl for read end") }
  if _,ok := pipe.WriteEnd().(types.HasFileDescriptorIf); !ok { t.Error("bad impl for write end") }
  if _,ok := pipe.WriteEnd().(io.ReaderFrom); !ok { t.Error("bad impl for write end") }
  //if _,ok := pipe.ReadEnd().(io.WriterTo); !ok { t.Error("bad impl for read end") }
}

func TestStartCmdWithPipedOutput_Echo(t *testing.T) {
  args := []string{ "echo", "-n", "salut" }
  ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
  defer cancel()

  done := runCmdGetOutputOrDie(ctx, t, args)

  select {
    case data := <-done:
      if string(data) != "salut" { t.Errorf("%v got: '%s'", args, data) }
    case <- ctx.Done():
      t.Fatalf("%v timedout", args)
  }
}

func TestStartCmdWithPipedOutput_Timeout(t *testing.T) {
  args := []string{ "sleep", "60" }
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()

  done := runCmdGetOutputOrDie(ctx, t, args)
  cancel()

  select {
    case <-done:
    case <-time.After(10 * time.Millisecond):
      t.Fatalf("%v did NOT timeout", args)
  }
}

