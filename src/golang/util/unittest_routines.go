package util

import "context"
import "encoding/base64"
import "encoding/json"
import "fmt"
import "io"
import "math/rand"
import "strings"
import "testing"
import "time"

func asJsonStrings(val interface{}, expected interface{}) (string, string) {
  var val_str, expected_str []byte
  var val_err, expected_err error
  val_str, val_err = json.MarshalIndent(val, "", "  ")
  expected_str, expected_err = json.MarshalIndent(expected, "", "  ")
  if val_err != nil || expected_err != nil {
    Fatalf("cannot marshal to json string: %v%v, %v/%v", val, val_err, expected, expected_err)
  }
  return string(val_str), string(expected_str)
}

func fmtAssertMsg(err_msg string, got string, expected string) string {
  const max_len = 1024
  got_limited := got
  expect_limited := expected
  if len(got_limited) > max_len { got_limited = got[:max_len] }
  if len(expect_limited) > max_len { expect_limited = expected[:max_len] }
  return fmt.Sprintf("%s:\ngot: %s\n !=\nexp: %s\n",
                     err_msg, got_limited, expect_limited)
}

func EqualsOrDie(err_msg string, val interface{}, expected interface{}) {
  val_str, expected_str := asJsonStrings(val, expected)
  if strings.Compare(val_str, expected_str) != 0 {
    Fatalf(fmtAssertMsg(err_msg, val_str, expected_str))
  }
}

func EqualsOrDieTest(t *testing.T, err_msg string, val interface{}, expected interface{}) {
  val_str, expected_str := asJsonStrings(val, expected)
  comp_res := strings.Compare(val_str, expected_str)
  if comp_res != 0 {
    t.Fatal(fmtAssertMsg(err_msg, val_str, expected_str))
  }
}

// Returns 0 if equal
func EqualsOrFailTest(t *testing.T, err_msg string, val interface{}, expected interface{}) int {
  val_str, expected_str := asJsonStrings(val, expected)
  comp_res := strings.Compare(val_str, expected_str)
  if comp_res != 0 {
    t.Error(fmtAssertMsg(err_msg, val_str, expected_str))
    return comp_res
  }
  return 0
}

func GenerateRandomTextData(size int) []byte {
  buffer := make([]byte, size)
  buffer_txt := make([]byte, base64.StdEncoding.EncodedLen(size))
  _, err := rand.Read(buffer)
  if err != nil { Fatalf("rand failed: %v", err) }
  base64.StdEncoding.Encode(buffer_txt, buffer)
  return buffer_txt[:size]
}

func ProduceRandomTextIntoPipe(ctx context.Context, chunk int, iterations int) io.ReadCloser {
  pipe := NewInMemPipe(ctx)

  go func() {
    defer pipe.WriteEnd().Close()
    for i:=0; ctx.Err() == nil && (i < iterations || iterations < 1); i+=1 {
      data := GenerateRandomTextData(chunk)
      _, err := pipe.WriteEnd().Write(data)
      if err != nil { return }
    }
  }()
  return pipe.ReadEnd()
}

func WaitForNoError(t *testing.T, ctx context.Context, done <-chan error) {
  if done == nil { t.Error("channel is nil"); return }
  if ctx.Err() != nil { t.Errorf("context expired before select"); return }
  select {
    case err,ok := <-done:
      if !ok { t.Errorf("channel closed") }
      if err != nil { t.Errorf("Error in channel: %v", err) }
    case <-ctx.Done(): t.Errorf("WaitForNoError timeout: %v", ctx.Err())
  }
}

func WaitMillisForNoError(t *testing.T, millis int, done <-chan error) {
  if done == nil { t.Error("channel is nil"); return }
  select {
    case err,ok := <-done:
      if !ok { t.Errorf("channel closed") }
      if err != nil { t.Errorf("Error in channel: %v", err) }
    case <-time.After(time.Duration(millis)*time.Millisecond):
      t.Errorf("WaitForNoError timeout."); return
  }
}

func WaitForClosure(t *testing.T, ctx context.Context, done <-chan error) {
  if done == nil { t.Error("channel is nil"); return }
  if ctx.Err() != nil { t.Errorf("context expired before select"); return }
  for { select {
    case _,ok := <-done: if !ok { return }
    case <-ctx.Done(): t.Errorf("WaitForClosure timeout."); return
  }}
}

func WaitMillisForClosure(t *testing.T, millis int, done <-chan error) {
  if done == nil { t.Error("channel is nil"); return }
  for { select {
    case _,ok := <-done: if !ok { return }
    case <-time.After(time.Duration(millis)*time.Millisecond):
      t.Errorf("WaitForClosure timeout."); return
  }}
}

