package util

import "context"
import "encoding/base64"
import "encoding/json"
import "math/rand"
import "strings"
import "testing"
import "btrfs_to_glacier/types"

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

func EqualsOrDie(val interface{}, expected interface{}) {
  val_str, expected_str := asJsonStrings(val, expected)
  if strings.Compare(val_str, expected_str) != 0 {
    Fatalf("\n%s\n !=\n%s", val_str, expected_str)
  }
}

func EqualsOrDieTest(t *testing.T, err_msg string, val interface{}, expected interface{}) {
  val_str, expected_str := asJsonStrings(val, expected)
  comp_res := strings.Compare(val_str, expected_str)
  if comp_res != 0 {
    t.Fatalf("%s:\ngot: %s\n !=\nexp: %s", err_msg, val_str, expected_str)
  }
}

// Returns 0 if equal
func EqualsOrFailTest(t *testing.T, val interface{}, expected interface{}) int {
  val_str, expected_str := asJsonStrings(val, expected)
  comp_res := strings.Compare(val_str, expected_str)
  if comp_res != 0 {
    t.Errorf("\n%s\n !=\n%s", val_str, expected_str)
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

func ProduceRandomTextIntoPipe(ctx context.Context, chunk int, iterations int) types.PipeReadEnd {
  var err error
  pipe := NewFileBasedPipe()
  defer CloseIfProblemo(pipe, &err)

  go func() {
    defer pipe.WriteEnd().Close()
    for i:=0; ctx.Err() == nil && i < iterations; i+=1 {
      data := GenerateRandomTextData(chunk)
      _, err := pipe.WriteEnd().Write(data)
      if err != nil {
        pipe.WriteEnd().PutErr(err)
        return
      }
    }
  }()
  return pipe.ReadEnd()
}

