package aws_s3_metadata

import (
  "context"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  //"btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
)

type S3Metadata struct {}

func NewMetadata(conf *pb.Config, aws_conf *aws.Config) (types.Metadata, error) {
  return nil, nil
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

