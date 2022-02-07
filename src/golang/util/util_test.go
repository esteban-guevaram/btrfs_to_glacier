package util

import "context"
import "io"
import "math/rand"
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
    case <-time.After(LargeTimeout):
      t.Fatalf("context timeout was not taken into account")
  }
}

func TestInMemPipeCtxCancel_WhileWriting(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), TestTimeout)
  defer cancel()
  pipe := NewInMemPipe(ctx)
  pipe_f := func(buf []byte) { pipe.WriteEnd().Write(buf) }
  testInMemPipeCtxCancel_Helper(t, pipe, pipe_f)
}

func TestFileBasedPipeCtxCancel_WhileWriting(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), TestTimeout)
  defer cancel()
  pipe := NewFileBasedPipe(ctx)
  pipe_f := func(buf []byte) { pipe.WriteEnd().Write(buf) }
  testInMemPipeCtxCancel_Helper(t, pipe, pipe_f)
}

func TestInMemPipeCtxCancel_WhileReading(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), TestTimeout)
  defer cancel()
  pipe := NewInMemPipe(ctx)
  pipe_f := func(buf []byte) { pipe.ReadEnd().Read(buf) }
  testInMemPipeCtxCancel_Helper(t, pipe, pipe_f)
}

func TestFileBasedPipeCtxCancel_WhileReading(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), TestTimeout)
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

func randomPipeImpl(t *testing.T, ctx context.Context) types.Pipe {
  if rand.Int() % 2 == 0 {
    t.Logf("NewInMemPipe")
    return NewInMemPipe(ctx)
  }
  t.Logf("NewFileBasedPipe")
  return NewFileBasedPipe(ctx)
}
func TestPipePropagateClosure_Fuzzer(t *testing.T) {
  if RaceDetectorOn { return }
  seed := time.Now().UnixNano()
  rand.Seed(seed)
  pipe_cnt := 1 + rand.Intn(7)
  chan_cnt := pipe_cnt + 1
  done := make(chan int)
  ctx,cancel := context.WithTimeout(context.Background(), TestTimeout)
  defer cancel()
  expect_count := make([]int, chan_cnt)
  pipes := make([]types.Pipe, pipe_cnt)
  var pipe_last types.Pipe = randomPipeImpl(t, ctx)
  message := GenerateRandomTextData(1024 + rand.Intn(3*4096))
  t.Logf("seed=%d, pipe_cnt=%d, len(message)=%d", seed, pipe_cnt, len(message))

  go func(pipe types.Pipe) {
    defer pipe.WriteEnd().Close()
    pipe.WriteEnd().Write(message)
    done <- 0
  }(pipe_last)

  copy_f := func(idx int, write io.WriteCloser, read io.ReadCloser) {
    defer write.Close()
    defer read.Close()
    io.Copy(write, read)
    done <- idx
  }
  for i,_ := range pipes {
    pipes[i] = randomPipeImpl(t, ctx)
    go copy_f(i+1, pipes[i].WriteEnd(), pipe_last.ReadEnd())
    pipe_last = pipes[i]
    expect_count[i+1] = i+1
  }

  var data []byte
  go func(pipe types.Pipe) {
    defer pipe.ReadEnd().Close()
    defer close(done)
    data,_ = io.ReadAll(pipe.ReadEnd())
  }(pipe_last)

  count := []int{}
  for i := range done { count = append(count, i) }
  EqualsOrFailTest(t, "count", count, expect_count)
  EqualsOrFailTest(t, "message bytes", data, message)
}

func TestClosedReadEndBehavior(t *testing.T) {
  ctx,cancel := context.WithTimeout(context.Background(), TestTimeout)
  defer cancel()
  close_test_f := func(scope string, writer io.WriteCloser, reader io.ReadCloser) {
    done := make(chan error)
    reader.Close()
    go func() { writer.Write([]byte("coucou")) }()
    go func() {
      defer close(done)
      cnt,err := reader.Read(make([]byte,1))
      if err == nil || err == io.EOF || cnt > 0 {
        t.Fatalf("%s.ReadEnd().Read should fail on a closed read end", scope)
      }
    }()
    WaitForClosure(t, ctx, done)
  }
  limit_wrap := func(reader io.ReadCloser) io.ReadCloser {
    reader.Close()
    return io.NopCloser(&io.LimitedReader{ R:reader, N:1, })
  }

  pipe_1 := NewInMemPipe(ctx)
  close_test_f("pipe_1", pipe_1.WriteEnd(), pipe_1.ReadEnd())
  pipe_2 := NewFileBasedPipe(ctx)
  close_test_f("pipe_2", pipe_2.WriteEnd(), pipe_2.ReadEnd())
  pipe_3 := NewInMemPipe(ctx)
  close_test_f("pipe_3", pipe_3.WriteEnd(), limit_wrap(pipe_3.ReadEnd()))
  pipe_4 := NewFileBasedPipe(ctx)
  close_test_f("pipe_4", pipe_4.WriteEnd(), limit_wrap(pipe_4.ReadEnd()))
}

func TestStartCmdWithPipedOutput_Echo(t *testing.T) {
  args := []string{ "echo", "-n", "salut" }
  ctx, cancel := context.WithTimeout(context.Background(), 2*TestTimeout)
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
    case <-time.After(TestTimeout):
      t.Fatalf("%v did NOT timeout", args)
  }
}

