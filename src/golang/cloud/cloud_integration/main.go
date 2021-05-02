package main

import (
  "context"
  "btrfs_to_glacier/cloud"
  //pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
)

func main() {
  util.Infof("cloud_integration run")

  var err error
  var aws_conf *aws.Config
  var metadata types.Metadata
  ctx := context.Background()
  conf := util.LoadTestConf()
  codec := new(types.MockCodec)
  aws_conf, err = cloud.NewAwsConfig(ctx, conf)
  if err != nil { util.Fatalf("%v", err) }
  metadata, err = cloud.NewMetadata(conf, aws_conf, codec)
  if err != nil { util.Fatalf("%v", err) }

  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("%v", err) }
    case <-ctx.Done():
  }
  util.Infof("ALL DONE")
}

