package main

import (
  "context"
  "errors"
  "fmt"
  "strconv"
  "time"

  "btrfs_to_glacier/cloud"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/dynamodb"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "google.golang.org/protobuf/proto"
)

func timedUuid(base_uuid string) string {
  return fmt.Sprintf("%s-%d", base_uuid, time.Now().UnixNano())
}

func dummyChunks(chunk_uuid string) *pb.SnapshotChunks {
  chunk := &pb.SnapshotChunks_Chunk {
    Uuid: chunk_uuid,
    Start: 0,
    Size: 3,
  }
  return &pb.SnapshotChunks{
    KeyFingerprint: "fp",
    Chunks: []*pb.SnapshotChunks_Chunk{chunk},
  }
}

func dummySubVolume(vol_uuid string) *pb.SubVolume {
 return &pb.SubVolume{
    Uuid: vol_uuid,
    MountedPath: "/monkey/biz",
    CreatedTs: 666,
    OriginSys: &pb.SystemInfo{
      KernMajor: 1,
      BtrfsUsrMajor: 1,
      ToolGitCommit: "commit_hash",
    },
  }
}

func dummySnapshot(snap_uuid string, vol_uuid string) *pb.SubVolume {
  vol := dummySubVolume(snap_uuid)
  vol.ParentUuid = vol_uuid
  vol.ReadOnly = true
  vol.CreatedTs += 111
  vol.GenAtCreation = 777
  return vol
}

func dummySnapshotSequence(vol_uuid string, seq_uuid string) *pb.SnapshotSequence {
  vol := dummySubVolume(vol_uuid)
  snap := fmt.Sprintf("%s_snap", vol_uuid)
  return &pb.SnapshotSequence{
    Uuid: seq_uuid,
    Volume: vol,
    SnapUuids: []string{snap},
  }
}

func TestMetadataSetup(ctx context.Context, conf *pb.Config, aws_conf *aws.Config, metadata types.Metadata) {
  client := dynamodb.NewFromConfig(*aws_conf)
  _, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
    TableName: &conf.Aws.DynamoDb.TableName,
  })

  if err != nil {
    apiErr := new(dyn_types.ResourceNotFoundException)
    if !errors.As(err, &apiErr) { util.Fatalf("%v", err) }
    util.Infof("TestMetadataSetup '%s' not exist", conf.Aws.DynamoDb.TableName)
  } else {
    waiter := dynamodb.NewTableNotExistsWaiter(client)
    wait_rq := &dynamodb.DescribeTableInput{ TableName: &conf.Aws.DynamoDb.TableName, }
    err = waiter.Wait(ctx, wait_rq, 30 * time.Second)
    if err != nil { util.Fatalf("%v", err) }
    util.Infof("TestMetadataSetup '%s' deleted", conf.Aws.DynamoDb.TableName)
  }

  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("%v", err) }
    case <-ctx.Done():
  }

  done = metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("Idempotent err: %v", err) }
    case <-ctx.Done():
  }
}

func TestStorageSetup(ctx context.Context, conf *pb.Config, client *s3.Client, storage types.Storage) {
  _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
    Bucket: &conf.Aws.S3.BucketName,
  })

  if err != nil {
    if !cloud.IsS3Error(new(s3_types.NoSuchBucket), err) {
      util.Fatalf("%v", err)
    }
    util.Infof("TestStorageSetup '%s' not exist", conf.Aws.S3.BucketName)
  } else {
    waiter := s3.NewBucketNotExistsWaiter(client)
    wait_rq := &s3.HeadBucketInput{ Bucket: &conf.Aws.S3.BucketName, }
    err = waiter.Wait(ctx, wait_rq, 30 * time.Second)
    if err != nil { util.Fatalf("%v", err) }
    util.Infof("TestStorageSetup '%s' deleted", conf.Aws.S3.BucketName)
  }

  done := storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("%v", err) }
    case <-ctx.Done():
  }

  done = storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("Not idempotent %v", err) }
    case <-ctx.Done():
  }
}

func TestAllStorage(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) {
  new_conf := proto.Clone(conf).(*pb.Config)
  // A bigger chunk, will make tests slow+expensive
  new_conf.Aws.S3.ChunkLen = 128*1024

  codec := new(types.MockCodec)
  codec.Fingerprint = types.PersistableString{"some_fp"}
  storage, err := cloud.NewStorage(new_conf, aws_conf, codec)
  client := s3.NewFromConfig(*aws_conf)
  if err != nil { util.Fatalf("%v", err) }

  //TestStorageSetup(ctx, new_conf, client, storage)
  TestAllS3ReadWrite(ctx, new_conf, client, storage)
}

func TestAllMetadata(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) {
  metadata, err := cloud.NewDelMetadata(conf, aws_conf)
  if err != nil { util.Fatalf("%v", err) }

  TestMetadataSetup(ctx, conf, aws_conf, metadata)
  TestAllDynamoDbReadWrite(ctx, metadata)
  TestAllDynamoDbDelete(ctx, metadata)
}

func TestCallerIdentity(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) {
  var err error
  var id_int int
  var account_id string
  account_id, err = cloud.GetAccountId(ctx, aws_conf)
  if err != nil { util.Fatalf("%v", err) }
  id_int, err = strconv.Atoi(account_id)
  if err != nil || id_int < 1 { util.Fatalf("invalid account id") }
}

func useUniqueInfrastructureNames(conf *pb.Config) *pb.Config {
  new_conf := proto.Clone(conf).(*pb.Config)
  new_conf.Aws.DynamoDb.TableName = fmt.Sprintf("%s%d", conf.Aws.DynamoDb.TableName,
                                                time.Now().Unix())
  new_conf.Aws.S3.BucketName = fmt.Sprintf("%s%d", conf.Aws.S3.BucketName,
                                           time.Now().Unix())
  return new_conf
}

func main() {
  util.Infof("cloud_integration run")

  ctx := context.Background()
  //conf := useUniqueInfrastructureNames(util.LoadTestConf())
  conf := util.LoadTestConf()

  aws_conf, err := cloud.NewAwsConfig(ctx, conf)
  if err != nil { util.Fatalf("%v", err) }

  //TestCallerIdentity(ctx, conf, aws_conf)
  TestAllStorage(ctx, conf, aws_conf)
  //TestAllMetadata(ctx, conf, aws_conf)
  util.Infof("ALL DONE")
}

