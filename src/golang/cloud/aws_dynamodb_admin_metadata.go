package cloud

import (
  "context"
  "errors"
  "fmt"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/aws"
  "github.com/aws/aws-sdk-go-v2/service/dynamodb"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

  "google.golang.org/protobuf/proto"
)

type dynamoAdminMetadata struct {
  *dynamoMetadata
}

func NewAdminMetadata(conf *pb.Config, aws_conf *aws.Config) (types.AdminMetadata, error) {
  meta, err := NewMetadata(conf, aws_conf)
  if err != nil { return nil, err }
  return &dynamoAdminMetadata{ meta.(*dynamoMetadata) }, nil
}

func (self *dynamoMetadata) describeTable(ctx context.Context, tabname string) (*dyn_types.TableDescription, error) {
  params := &dynamodb.DescribeTableInput{
    TableName: &self.conf.Aws.DynamoDb.TableName,
  }
  result, err := self.client.DescribeTable(ctx, params)
  if err != nil {
    apiErr := new(dyn_types.ResourceNotFoundException)
    if errors.As(err, &apiErr) { util.Debugf("'%s' does not exist", tabname) }
    return nil, err
  }
  return result.Table, nil
}

func (self *dynamoMetadata) waitForTableCreation(ctx context.Context, tabname string) (<-chan error) {
  done := make(chan error, 1)
  go func() {
    defer close(done)
    ticker := time.NewTicker(self.describe_retry)
    defer ticker.Stop()

    for {
      select {
        case <-ticker.C:
          result, err := self.describeTable(ctx, tabname)
          if err != nil {
            done <- err
            return
          }
          if result.TableStatus == dyn_types.TableStatusActive {
            done <- nil
            return
          }
          if result.TableStatus != dyn_types.TableStatusCreating {
            done <- fmt.Errorf("Unexpected status while waiting for table creation: %v", result.TableStatus)
            return
          }
        case <-ctx.Done():
          done <- fmt.Errorf("Timedout while waiting for table creation")
          return
      }
    }
  }()
  return done
}

func (self *dynamoMetadata) SetupMetadata(ctx context.Context) (<-chan error) {
  tabname := self.conf.Aws.DynamoDb.TableName

  attrs := []dyn_types.AttributeDefinition{
    dyn_types.AttributeDefinition{
      AttributeName: &self.uuid_col,
      AttributeType: dyn_types.ScalarAttributeTypeS,
    },
    dyn_types.AttributeDefinition{
      AttributeName: &self.type_col,
      AttributeType: dyn_types.ScalarAttributeTypeS,
    },
  }
  schema := []dyn_types.KeySchemaElement{
    dyn_types.KeySchemaElement{
      AttributeName: &self.uuid_col,
      KeyType: dyn_types.KeyTypeHash,
    },
    dyn_types.KeySchemaElement{
      AttributeName: &self.type_col,
      KeyType: dyn_types.KeyTypeRange,
    },
  }
  params := &dynamodb.CreateTableInput{
    TableName: &tabname,
    AttributeDefinitions: attrs,
    KeySchema: schema,
    BillingMode: dyn_types.BillingModePayPerRequest,
  }

  result, err := self.client.CreateTable(ctx, params)
  if err != nil {
    apiErr := new(dyn_types.ResourceInUseException)
    if errors.As(err, &apiErr) {
      util.Infof("Table '%s' already exists", tabname)
      return util.WrapInChan(nil)
    }
    return util.WrapInChan(err)
  }
  if result.TableDescription.TableStatus == dyn_types.TableStatusActive {
    return util.WrapInChan(nil)
  }
  return self.waitForTableCreation(ctx, tabname)
}

func (self *dynamoAdminMetadata) DeleteObject(
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

func (self *dynamoAdminMetadata) DeleteSnapshotSeqHead(
    ctx context.Context, uuid string) error {
  if len(uuid) < 1 { return fmt.Errorf("DeleteSnapshotSeqHead: uuid is nil") }
  err := self.DeleteObject(ctx, uuid, &pb.SnapshotSeqHead{})
  if err == nil {
    util.Infof("Delete head: %v", uuid)
  }
  return err
}

func (self *dynamoAdminMetadata) DeleteSnapshotSeq(
    ctx context.Context, uuid string) error {
  if len(uuid) < 1 { return fmt.Errorf("DeleteSnapshotSeq: uuid is nil") }
  err := self.DeleteObject(ctx, uuid, &pb.SnapshotSequence{})
  if err == nil {
    util.Infof("Delete sequence: %v", uuid)
  }
  return err
}

func (self *dynamoAdminMetadata) DeleteSnapshot(
    ctx context.Context, uuid string) error {
  if len(uuid) < 1 { return fmt.Errorf("DeleteSnapshot: uuid is nil") }
  err := self.DeleteObject(ctx, uuid, &pb.SubVolume{})
  if err == nil {
    util.Infof("Delete snapshot: %v", uuid)
  }
  return err
}

