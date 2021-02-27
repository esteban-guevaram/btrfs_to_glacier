package messages

import (
  "testing"
  "google.golang.org/protobuf/encoding/prototext"
)

func TestMessagesGotGenerated(t *testing.T) {
  vol := SubVolume {
    Uuid: "salut",
    MountedPath: "/choco/pops",
    CreatedTs: 666,
    OriginSys: &SystemInfo{
      KernMajor: 5,
      KernMinor: 10,
    },
  }
  txt, err := prototext.Marshal(&vol)
  t.Logf("TextMarshaler vol='%s' (err:%v)", txt, err)
  t.Logf("String vol='%s'", vol.String())
  t.Logf("Format%%s vol='%s'", &vol)
  t.Logf("Format%%v vol='%v'", vol)
}

