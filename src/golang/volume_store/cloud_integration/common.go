package main

import (
  "bytes"
  "context"
  "fmt"
  "io"
  "time"

  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/service/s3"
)

func timedUuid(base_uuid string) string {
  return fmt.Sprintf("%s-%d", base_uuid, time.Now().UnixNano())
}

func GetObject(
    ctx context.Context, client *s3.Client, bucket string, key string) ([]byte, error) {
  in := &s3.GetObjectInput{
    Bucket: &bucket,
    Key: &key,
  }
  out, err := client.GetObject(ctx, in)
  if err != nil { return nil, err }

  defer out.Body.Close()
  var data []byte
  data, err = io.ReadAll(out.Body)
  if err == nil { util.Infof("Got object '%s' size=%d", key, len(data)) }
  return data, err
}

func PutObject(
    ctx context.Context, client *s3.Client, bucket string, key string, data []byte) error {
  reader := bytes.NewReader(data)
  in := &s3.PutObjectInput{
    Bucket: &bucket,
    Key: &key,
    Body: reader,
  }
  _, err := client.PutObject(ctx, in)
  return err
}

func GetObjectOrDie(
    ctx context.Context, client *s3.Client, bucket string, key string) []byte {
  data, err := GetObject(ctx, client, bucket, key)
  if err != nil { util.Fatalf("Failed to get object '%s': %v", key, err) }
  return data
}

func PutObjectOrDie(
    ctx context.Context, client *s3.Client, bucket string, key string, data []byte) {
  err := PutObject(ctx, client, bucket, key, data)
  if err != nil { util.Fatalf("Failed to put object '%s': %v", key, err) }
}

func DeleteBucket(ctx context.Context, client *s3.Client, bucket string) error {
  err := EmptyBucket(ctx, client, bucket)
  if err != nil { return err }
  _, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{ Bucket: &bucket, })
  return err
}

func EmptyBucketOrDie(ctx context.Context, client *s3.Client, bucket string) {
  err := EmptyBucket(ctx, client, bucket)
  if err != nil { util.Fatalf("Failed empty object: %v", err) }
}

func EmptyBucket(ctx context.Context, client *s3.Client, bucket string) error {
  list_in := &s3.ListObjectsV2Input{
    Bucket: &bucket,
    MaxKeys: 1000,
  }
  out, err := client.ListObjectsV2(ctx, list_in)
  if err != nil { return err }
  for _,obj := range out.Contents {
    del_in := &s3.DeleteObjectInput{
      Bucket: &bucket,
      Key: obj.Key,
    }
    _, err = client.DeleteObject(ctx, del_in)
    if err != nil { return err }
  }
  if out.ContinuationToken != nil { return fmt.Errorf("needs more iterations to delete all") }
  return nil
}

func DeleteBucketOrDie(ctx context.Context, client *s3.Client, bucket string) {
  err := DeleteBucket(ctx, client, bucket)
  if err != nil { util.Fatalf("Failed bucket delete: %v", err) }
}

