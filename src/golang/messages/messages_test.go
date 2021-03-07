package messages

import (
  "testing"
  "google.golang.org/protobuf/encoding/prototext"
  "google.golang.org/protobuf/proto"
)

func TestShallowCopy(t *testing.T) {
  sysinfo := &SystemInfo {
    KernMajor: 1,
    KernMinor: 2,
    BtrfsUsrMajor: 3,
    BtrfsUsrMinor: 4,
    ToolGitCommit: "hash",
  }
  clone_sys := *sysinfo
  // This just points to the same value as sysinfo
  //clone_sys2 := &(*sysinfo)
  clone_sys.KernMajor = 666
  clone_sys.ToolGitCommit = "other"
  if proto.Equal(&clone_sys, sysinfo) {
    t.Errorf("\n%s\n ==\n %s", &clone_sys, sysinfo)
  }
}

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

func TestConfigTextMarshal(t *testing.T) {
  conf := Config {
    RootSnapPath: "/choco/lat",
    SubvolPaths: []string { "/coco/loco", },
  }
  var loadedConf Config
  txt, _ := prototext.Marshal(&conf)
  prototext.Unmarshal(txt, &loadedConf)
  if !proto.Equal(&loadedConf, &conf) {
    t.Errorf("\n%s\n !=\n %s", &loadedConf, &conf)
  }
}

