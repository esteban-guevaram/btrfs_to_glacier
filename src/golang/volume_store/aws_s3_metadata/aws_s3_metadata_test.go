package aws_s3_metadata

import (
  "context"
  "fmt"
  "testing"
  "time"

  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/util"
  "btrfs_to_glacier/volume_store/mem_only"

  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "github.com/google/uuid"
  "google.golang.org/protobuf/proto"
)

func buildTestMetadataWithConf(t *testing.T, conf *pb.Config) (*S3Metadata, *s3_common.MockS3Client) {
  client := &s3_common.MockS3Client {
    AccountId: "some_random_string",
    Data: make(map[string][]byte),
    Class: make(map[string]s3_types.StorageClass),
    RestoreStx: make(map[string]string),
    Buckets: make(map[string]bool),
    HeadAlwaysEmpty: false,
    HeadAlwaysAccessDenied: false,
  }
  aws_conf, err := util.NewAwsConfig(context.TODO(), conf)
  if err != nil { t.Fatalf("Failed aws config: %v", err) }
  common, err := s3_common.NewS3Common(conf, aws_conf, client)
  if err != nil { t.Fatalf("Failed build common setup: %v", err) }
  common.BucketWait = 10 * time.Millisecond
  common.AccountId = client.AccountId

  meta := &S3Metadata{
    Metadata: &mem_only.Metadata{
      Conf: conf,
      State: &pb.AllMetadata{},
    },
    AwsConf: aws_conf,
    Client: client,
    Common: common,
  }
  meta.injectConstants()
  return meta, client
}

func buildTestMetadataWithState(t *testing.T, state *pb.AllMetadata) (*S3Metadata, *s3_common.MockS3Client) {
  var err error
  conf := util.LoadTestConf()
  meta, client := buildTestMetadataWithConf(t, conf)
  client.Buckets[conf.Aws.S3.MetadataBucketName] = true
  err = client.PutProto(meta.Key, state, s3_types.StorageClassStandard, false)
  if err != nil { t.Fatalf("failed to set init state: %v", err) }
  meta.State = state
  return meta, client
}

func TestLoadPreviousStateFromS3_NoBucket(t *testing.T) {
  conf := util.LoadTestConf()
  meta, client := buildTestMetadataWithConf(t, conf)
  meta.State = nil
  meta.LoadPreviousStateFromS3(context.TODO())
  util.EqualsOrFailTest(t, "Bad bucket", client.Buckets[conf.Aws.S3.MetadataBucketName], false)
  util.EqualsOrFailTest(t, "Bad object", client.Data[meta.Key], nil)
  mem_only.CompareStates(t, "expected empty state", meta.State, &pb.AllMetadata{})
}

func TestLoadPreviousStateFromS3_NoKey(t *testing.T) {
  meta, client := buildTestMetadataWithState(t, &pb.AllMetadata{})
  meta.State = nil
  client.DelObject(meta.Key)
  meta.LoadPreviousStateFromS3(context.TODO())
  util.EqualsOrFailTest(t, "Bad object", client.Data[meta.Key], nil)
  mem_only.CompareStates(t, "expected empty state", meta.State, &pb.AllMetadata{})
}

func TestLoadPreviousStateFromS3_PreviousState(t *testing.T) {
  _, expect_state := util.DummyAllMetadata()
  meta,_ := buildTestMetadataWithState(t, expect_state)
  meta.State = nil
  meta.LoadPreviousStateFromS3(context.TODO())
  mem_only.CompareStates(t, "expected empty state", meta.State, expect_state)
}

func TestSaveCurrentStateToS3_NoPrevState(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, expect_state := util.DummyAllMetadata()
  meta, client := buildTestMetadataWithState(t, proto.Clone(expect_state).(*pb.AllMetadata))
  client.DelObject(meta.Key)

  version, err := meta.SaveCurrentStateToS3(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
  if len(version) < 1 { t.Errorf("empty version") }
  persisted_state := &pb.AllMetadata{}
  err = client.GetProto(meta.Key, persisted_state)
  if err != nil { t.Errorf("client.GetProto error: %v", err) }
  util.EqualsOrFailTest(t, "Bad state", persisted_state, expect_state)
}

func TestSaveCurrentStateToS3_WithPrevState(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  vol_uuid, prev_state := util.DummyAllMetadata()
  var expect_state pb.AllMetadata = *prev_state
  meta, client := buildTestMetadataWithState(t, prev_state)

  new_seq := util.DummySnapshotSequence(vol_uuid, uuid.NewString())
  head, err := meta.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Fatalf("RecordSnapshotSeqHead error: %v", err) }
  expect_state.Heads[0] = head

  version, err := meta.SaveCurrentStateToS3(ctx)
  if err != nil { t.Errorf("Returned error: %v", err) }
  if len(version) < 1 { t.Errorf("empty version") }
  persisted_state := &pb.AllMetadata{}
  err = client.GetProto(meta.Key, persisted_state)
  if err != nil { t.Errorf("client.GetProto error: %v", err) }
  util.EqualsOrFailTest(t, "Bad state", persisted_state, expect_state)
}

func TestSaveCurrentStateToS3_Err(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  _, prev_state := util.DummyAllMetadata()
  meta, client := buildTestMetadataWithState(t, prev_state)
  client.Err = fmt.Errorf("TestSaveCurrentStateToS3_Err")

  _, err := meta.SaveCurrentStateToS3(ctx)
  if err == nil { t.Errorf("Expected error got: %v", err) }
}
