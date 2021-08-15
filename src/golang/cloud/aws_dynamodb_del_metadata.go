package cloud

import (
  "context"
  "errors"
  "fmt"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/dynamodb"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

  "google.golang.org/protobuf/proto"
)

type dynamoDelMetadata struct {
  *dynamoMetadata
}

func NewDelMetadata(conf *pb.Config, aws_conf *aws.Config) (types.DeleteMetadata, error) {
  meta, err := NewMetadata(conf, aws_conf)
  if err != nil { return nil, err }
  return &dynamoDelMetadata{ meta.(*dynamoMetadata) }, nil
}

func (self *dynamoDelMetadata) DeleteObject(
  ctx context.Context, uuid string, msg proto.Message) error {
  // We use a condition expression to trigger an error in case the key does not exist.
  // Otherwise we cannot distinguish between the item not existing and a successful delete.
  condition_expr := fmt.Sprintf("attribute_exists(%s)", blob_col)
  params := &dynamodb.DeleteItemInput{
    TableName: &self.conf.Aws.DynamoDb.TableName,
    Key: getItemKey(uuid, msg),
    ConditionExpression: &condition_expr,
    ReturnConsumedCapacity: dyn_types.ReturnConsumedCapacityNone,
    ReturnValues: dyn_types.ReturnValueNone,
  }
  _, err := self.client.DeleteItem(ctx, params)
  if err != nil {
    apiErr := new(dyn_types.ConditionalCheckFailedException)
    if errors.As(err, &apiErr) {
      return fmt.Errorf("%w uuid=%v", types.ErrNotFound, uuid)
    }
  }
  return err
}

func (self *dynamoDelMetadata) DeleteSnapshotSeqHead(
    ctx context.Context, uuid string) error {
  if len(uuid) < 1 { return fmt.Errorf("DeleteSnapshotSeqHead: uuid is nil") }
  err := self.DeleteObject(ctx, uuid, &pb.SnapshotSeqHead{})
  if err == nil {
    util.Infof("Delete head: %v", uuid)
  }
  return err
}

func (self *dynamoDelMetadata) DeleteSnapshotSeq(
    ctx context.Context, uuid string) error {
  if len(uuid) < 1 { return fmt.Errorf("DeleteSnapshotSeq: uuid is nil") }
  err := self.DeleteObject(ctx, uuid, &pb.SnapshotSequence{})
  if err == nil {
    util.Infof("Delete sequence: %v", uuid)
  }
  return err
}

func (self *dynamoDelMetadata) DeleteSnapshot(
    ctx context.Context, uuid string) error {
  if len(uuid) < 1 { return fmt.Errorf("DeleteSnapshot: uuid is nil") }
  err := self.DeleteObject(ctx, uuid, &pb.SubVolume{})
  if err == nil {
    util.Infof("Delete snapshot: %v", uuid)
  }
  return err
}

