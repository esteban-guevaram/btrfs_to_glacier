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
    TreePath: "/pops",
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
  source := &Source{
    Type: Source_BTRFS,
    Paths: []*Source_VolSnapPathPair{
      &Source_VolSnapPathPair{
        VolPath: "/tmp/subvol1",
        SnapPath: "/tmp/snaps",
      },
    },
    History: &Source_SnapHistory{
      DaysKeepAll: 30,
      KeepOnePeriodDays: 30,
    },
  }
  conf := Config {
    Sources: []*Source{ source, },
    Aws: &Aws {
      AccessKeyId: "coucou",
      SecretAccessKey: "coucou",
      Region: "eu-central-1", // needs to be valid for unittests
      DynamoDb: &Aws_DynamoDb{ TableName: "coucou", },
      S3: &Aws_S3{ BucketName: "coucou", ChunkLen: 1024*1024, },
    },
  }
  var loadedConf Config
  txt, _ := prototext.Marshal(&conf)
  prototext.Unmarshal(txt, &loadedConf)
  if !proto.Equal(&loadedConf, &conf) {
    t.Errorf("\n%s\n !=\n %s", &loadedConf, &conf)
  }
}

