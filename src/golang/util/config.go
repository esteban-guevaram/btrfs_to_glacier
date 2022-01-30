package util

import (
  "os"
  "flag"
  "io/fs"
  fpmod "path/filepath"

  pb "btrfs_to_glacier/messages"
)

var access_flag string
var secret_flag string
var session_flag string
var region_flag string
var table_flag string
var store_bucket_flag string
var meta_bucket_flag string

func init() {
  flag.StringVar(&access_flag,       "access",       "", "Access Key ID")
  flag.StringVar(&secret_flag,       "secret",       "", "Secret access key")
  flag.StringVar(&session_flag,      "session",      "", "Session token for temporal credentials")
  flag.StringVar(&region_flag,       "region",       "", "Default AWS region")
  flag.StringVar(&table_flag,        "table",        "", "Dynamodb table name")
  flag.StringVar(&store_bucket_flag, "store_bucket", "", "S3 storage bucket name")
  flag.StringVar(&meta_bucket_flag,  "meta_bucket",  "", "S3 metadata bucket name")
}

func Load() (*pb.Config, error) {
  conf := &pb.Config{}
  overwriteWithFlags(conf)
  err := Validate(conf)
  return conf, err
}

func overwriteWithFlags(conf *pb.Config) {
  flag.Parse()
  if access_flag       != "" { conf.Aws.AccessKeyId = access_flag }
  if secret_flag       != "" { conf.Aws.SecretAccessKey = secret_flag }
  if session_flag      != "" { conf.Aws.SessionToken = session_flag }
  if region_flag       != "" { conf.Aws.Region = region_flag }
  if table_flag        != "" { conf.Aws.DynamoDb.TableName = table_flag }
  if store_bucket_flag != "" { conf.Aws.S3.StorageBucketName = store_bucket_flag }
  if meta_bucket_flag  != "" { conf.Aws.S3.MetadataBucketName = meta_bucket_flag }
}

func TestSimpleDirLocalFs() (*pb.LocalFs, func()) {
  local_fs_dir, err := os.MkdirTemp(os.TempDir(), "localfs_")
  if err != nil { Fatalf("failed to create tmp dir: %v", err) }
  err = os.Mkdir(fpmod.Join(local_fs_dir, "metadata"), fs.ModePerm)
  if err != nil { Fatalf("failed to create dir: %v", err) }
  err = os.Mkdir(fpmod.Join(local_fs_dir, "storage"), fs.ModePerm)
  if err != nil { Fatalf("failed to create dir: %v", err) }

  part := &pb.LocalFs_Partition{
    FsUuid: "fs_uuid",
    MountRoot: local_fs_dir,
    MetadataDir: "metadata",
    StorageDir: "storage",
  }
  local_fs := &pb.LocalFs{
    Sinks: []*pb.LocalFs_RoundRobin{
      &pb.LocalFs_RoundRobin{ Partitions: []*pb.LocalFs_Partition{ part, }, },
    },
  }
  return local_fs, func() { CleanLocalFs(local_fs) }
}

func CleanLocalFs(local_fs *pb.LocalFs) {
  tmp_root := os.TempDir()
  for _,g := range local_fs.Sinks {
    for _,p := range g.Partitions {
      if !fpmod.HasPrefix(p.MountRoot, tmp_root) { continue }
      os.RemoveAll(p.MountRoot)
    }
  }
}

func LoadTestConfWithLocalFs(local_fs *pb.LocalFs) *pb.Config {
  source := &pb.Source{
    Type: pb.Source_BTRFS,
    Paths: []*pb.Source_VolSnapPathPair{
      &pb.Source_VolSnapPathPair{
        VolPath: "/tmp/subvol1",
        SnapPath: "/tmp/snaps",
      },
    },
    History: &pb.Source_SnapHistory{
      DaysKeepAll: 30,
      KeepOnePeriodDays: 30,
    },
  }
  conf := pb.Config {
    Sources: []*pb.Source{ source, },
    Aws: &pb.Aws {
      AccessKeyId: "coucou",
      SecretAccessKey: "coucou",
      Region: "eu-central-1", // needs to be valid for unittests
      DynamoDb: &pb.Aws_DynamoDb{ TableName: "coucou", },
      S3: &pb.Aws_S3{
        StorageBucketName: "coucou_store",
        MetadataBucketName: "coucou_meta",
        ChunkLen: 1024*1024,
      },
    },
    LocalFs: local_fs,
  }
  overwriteWithFlags(&conf)
  return &conf
}

func LoadTestConf() *pb.Config {
  return LoadTestConfWithLocalFs(nil)
}

func Validate(conf *pb.Config) error {
  // Each subvolume should be mentioned in only 1 source
  // Sources, Stores, Restores, Workflows, Tools have a unique name
  // Reference by name not dangling
  return nil
}

