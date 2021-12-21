package main

import (
  "context"

  meta "btrfs_to_glacier/volume_store/aws_s3_metadata"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"

  "google.golang.org/protobuf/proto"
)

type s3MetaReadWriteTester struct {
  Conf *pb.Config
  Client *s3.Client
  Metadata types.AdminMetadata
}

func TestAllS3MetaReadWrite(
    ctx context.Context, conf *pb.Config, client *s3.Client, metadata types.AdminMetadata) {
}

func TestAllS3Metadata(ctx context.Context, conf *pb.Config, aws_conf *aws.Config) {
  new_conf := proto.Clone(conf).(*pb.Config)

  metadata, err := meta.NewMetadataAdmin(new_conf, aws_conf)
  client := meta.TestOnlyGetInnerClientToAvoidConsistencyFails(metadata)
  if err != nil { util.Fatalf("%v", err) }

  suite := s3MetaAdminTester {
    &s3MetaReadWriteTester{ Conf: conf, Client: client, Metadata: metadata, },
  }

  suite.TestS3MetadataSetup(ctx)
  DeleteBucketOrDie(ctx, client, new_conf.Aws.S3.MetadataBucketName)
}

