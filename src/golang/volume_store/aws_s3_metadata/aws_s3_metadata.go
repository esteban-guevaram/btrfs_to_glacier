package aws_s3_metadata

import (
  "context"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  //"btrfs_to_glacier/util"
  s3_common "btrfs_to_glacier/volume_store/aws_s3_common"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"
)

// The subset of the s3 client used.
// Convenient for unittesting purposes.
type usedS3If interface {
  PutBucketVersioning(
    context.Context, *s3.PutBucketVersioningInput, ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error)
  PutBucketLifecycleConfiguration(
    context.Context, *s3.PutBucketLifecycleConfigurationInput, ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error)
}

type S3Metadata struct {
  Conf       *pb.Config
  AwsConf    *aws.Config
  Common     *s3_common.S3Common
  Client      usedS3If
}

func NewMetadata(conf *pb.Config, aws_conf *aws.Config) (types.Metadata, error) {
  client := s3.NewFromConfig(*aws_conf)
  common, err := s3_common.NewS3Common(conf, aws_conf, client)
  if err != nil { return nil, err }

  metadata := &S3Metadata{
    Conf: conf,
    AwsConf: aws_conf,
    Client: client,
    Common: common,
  }
  metadata.injectConstants()
  return metadata, nil
}

func (self *S3Metadata) injectConstants() {
}

func (self *S3Metadata) RecordSnapshotSeqHead(
    ctx context.Context, new_seq *pb.SnapshotSequence) (*pb.SnapshotSeqHead, error) {
  return nil, nil
}

func (self *S3Metadata) AppendSnapshotToSeq(
    ctx context.Context, seq *pb.SnapshotSequence, snap *pb.SubVolume) (*pb.SnapshotSequence, error) {
  return nil, nil
}

func (self *S3Metadata) AppendChunkToSnapshot(
    ctx context.Context, snap *pb.SubVolume, data *pb.SnapshotChunks) (*pb.SubVolume, error) {
  return nil, nil
}

func (self *S3Metadata) ReadSnapshotSeqHead(
    ctx context.Context, uuid string) (*pb.SnapshotSeqHead, error) {
return nil, nil
}

func (self *S3Metadata) ReadSnapshotSeq(
    ctx context.Context, uuid string) (*pb.SnapshotSequence, error) {
return nil, nil
}

func (self *S3Metadata) ReadSnapshot(
    ctx context.Context, uuid string) (*pb.SubVolume, error) {
return nil, nil
}

func (self *S3Metadata) ListAllSnapshotSeqHeads(
    ctx context.Context) (types.SnapshotSeqHeadIterator, error) {
return nil, nil
}

func (self *S3Metadata) ListAllSnapshotSeqs(
    ctx context.Context) (types.SnapshotSequenceIterator, error) {
return nil, nil
}

func (self *S3Metadata) ListAllSnapshots(
    ctx context.Context) (types.SnapshotIterator, error) {
return nil, nil
}

