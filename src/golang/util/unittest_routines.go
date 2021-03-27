package util

import "encoding/json"
import "strings"
import "testing"

func CompareAsStrings(t *testing.T, val interface{}, expected interface{}) {
  var val_str, expected_str []byte
  var val_err, expected_err error
  val_str, val_err = json.MarshalIndent(val, "", "  ")
  expected_str, expected_err = json.MarshalIndent(expected, "", "  ")
  if val_err != nil || expected_err != nil { panic("cannot marshal to json string") }
  if strings.Compare(string(val_str), string(expected_str)) != 0 {
    t.Errorf("\n%s\n !=\n%s", val_str, expected_str)
  }
}

