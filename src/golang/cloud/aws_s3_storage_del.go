package cloud

import (
  "context"
  "fmt"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/s3"
  s3_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
  // see https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#Client.DeleteObjects
  delete_objects_max = 1000
)

type s3DeleteStorage struct {
  *s3Storage
}

func NewDeleteStorage(conf *pb.Config, aws_conf *aws.Config, codec types.Codec) (types.Storage, error) {
  storage, err := NewStorage(conf, aws_conf, codec)
  if err != nil { return nil, err }

  del_storage := &s3DeleteStorage{ s3Storage: storage.(*s3Storage), }
  return del_storage, nil
}

func (self *s3DeleteStorage) deleteBatch(
    ctx context.Context, low_bound int, up_bound int, chunks *pb.SnapshotChunks) error {
  del_in := &s3.DeleteObjectsInput{
    Bucket: &self.conf.Aws.S3.BucketName,
    Delete: &s3_types.Delete{
      Objects: make([]s3_types.ObjectIdentifier, up_bound-low_bound),
      Quiet: true,
    },
  }
  for i:=low_bound; i<up_bound; i+=1 {
    del_in.Delete.Objects[i-low_bound].Key = &chunks.Chunks[i].Uuid
  }
  del_out,err := self.client.DeleteObjects(ctx, del_in)
  if err != nil { return err }
  if len(del_out.Errors) > 0 { return fmt.Errorf("failed to delete %d keys", len(del_out.Errors)) }
  return nil
}

func (self *s3DeleteStorage) DeleteChunks(ctx context.Context, chunks *pb.SnapshotChunks) (<-chan error) {
  var err error
  done := make(chan error, 1)
  defer func() { util.OnlyCloseChanWhenError(done, err) }()
  if len(chunks.Chunks) < 1 { err = fmt.Errorf("cannot delete 0 keys"); return done }

  go func() {
    var err error
    defer close(done)
    for low_bound:=0; low_bound<len(chunks.Chunks); low_bound+=delete_objects_max {
      up_bound := low_bound + delete_objects_max
      if up_bound > len(chunks.Chunks) { up_bound = len(chunks.Chunks) }
      err := self.deleteBatch(ctx, low_bound, up_bound, chunks)
      if err != nil { break }
    }
    util.Infof("Deleted %d keys: '%s'...'%s'",
               len(chunks.Chunks), chunks.Chunks[0].Uuid, chunks.Chunks[len(chunks.Chunks)-1].Uuid)
    done <- err
  }()
  return done
}

