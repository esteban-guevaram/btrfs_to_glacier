package util

import "context"
import "io/ioutil"
import "testing"
import "time"

func runCmdGetOutputOrDie(ctx context.Context, t *testing.T, args []string) <-chan []byte {
  read_end, err := StartCmdWithPipedOutput(ctx, args)
  if err != nil {
    t.Fatalf("%v failed: %v", args, err)
  }

  done := make(chan []byte, 1)
  go func() {
    defer read_end.Close()
    data, _ := ioutil.ReadAll(read_end)
    done <- data
  }()

  return done
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

