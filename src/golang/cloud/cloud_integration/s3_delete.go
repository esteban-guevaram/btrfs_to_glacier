package main

import (
  "context"
  "time"

  "btrfs_to_glacier/cloud"
  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"

  "github.com/google/uuid"
)

type s3AdminTester struct { *s3ReadWriteTester }

func TestS3StorageSetup(ctx context.Context, conf *pb.Config, client *s3.Client, storage types.AdminStorage) {
  err := deleteBucket(ctx, conf, client)

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
      util.Infof("Bucket '%s' created OK", conf.Aws.S3.BucketName)
    case <-ctx.Done():
  }

  done = storage.SetupStorage(ctx)
  select {
    case err := <-done:
      if err != nil { util.Fatalf("Not idempotent %v", err) }
    case <-ctx.Done():
  }
}

func (self *s3AdminTester) testDeleteChunks_Helper(ctx context.Context, obj_count int) {
  keys := make([]string, obj_count)
  for i:=0; i<obj_count; i+=1 {
    keys[i],_ = self.putRandomObjectOrDie(ctx, 1024)
  }
  chunks := &pb.SnapshotChunks{
    Chunks: make([]*pb.SnapshotChunks_Chunk, obj_count),
  }
  for i,key := range keys {
    chunks.Chunks[i] = &pb.SnapshotChunks_Chunk{ Uuid:key, }
  }

  done := self.Storage.DeleteChunks(ctx, chunks)
  err := util.WaitForClosureOrDie(ctx, done)
  if err != nil { util.Fatalf("delete failed: %v", err) }

  for _,key := range keys {
    _,err := self.getObject(ctx, key)
    if !cloud.IsS3Error(new(s3_types.NoSuchKey), err) {
      util.Fatalf("Key '%s' was not deleted", key)
    }
  }
}

func (self *s3AdminTester) TestDeleteChunks_Single(ctx context.Context) {
  self.testDeleteChunks_Helper(ctx, 1)
}

func (self *s3AdminTester) TestDeleteChunks_Multi(ctx context.Context) {
  self.testDeleteChunks_Helper(ctx, 3)
}

func (self *s3AdminTester) TestDeleteChunks_NoSuchKey(ctx context.Context) {
  key := uuid.NewString()
  uuids := []*pb.SnapshotChunks_Chunk{
    &pb.SnapshotChunks_Chunk{ Uuid:key, },
  }
  chunks := &pb.SnapshotChunks{ Chunks: uuids, }
  done := self.Storage.DeleteChunks(ctx, chunks)
  err := util.WaitForClosureOrDie(ctx, done)
  if err != nil { util.Fatalf("delete of unexisting object should be a noop: %v", err) }
}

func TestAllS3Delete(ctx context.Context, conf *pb.Config, client *s3.Client, storage types.AdminStorage) {
  suite := s3AdminTester{
    &s3ReadWriteTester{ Conf:conf, Client:client, Storage:storage, },
  }

  suite.TestDeleteChunks_Single(ctx)
  suite.TestDeleteChunks_Multi(ctx)
  suite.TestDeleteChunks_NoSuchKey(ctx)
}

