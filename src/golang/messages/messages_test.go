package messages

import (
  "testing"
  "github.com/golang/protobuf/proto"
)

func TestMessagesGotGenerated(t *testing.T) {
  vol := Volume { Uuid: "salut" }
  t.Logf("TextMarshaler vol='%s'", proto.MarshalTextString(&vol))
  t.Logf("String vol='%s'", vol.String())
  t.Logf("Format vol='%v'", vol)
}

