package aws_s3_metadata

import (
  "context"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  //"btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
)

type S3AdminMetadata struct { *S3Metadata }

func NewMetadataAdmin(conf *pb.Config, aws_conf *aws.Config) (types.AdminMetadata, error) {
  return nil, nil
}

func (self *S3AdminMetadata) SetupMetadata(
    ctx context.Context) (<-chan error) {
  return nil
}

func (self *S3AdminMetadata) DeleteMetadataUuids(
    ctx context.Context, seq_uuids []string, snap_uuids []string) (<-chan error) {
  return nil
}

func (self *S3AdminMetadata) ReplaceSnapshotSeqHead(
    ctx context.Context, head *pb.SnapshotSeqHead) (*pb.SnapshotSeqHead, error) {
  return nil, nil
}

