package cloud
// * Can be used as KV store but blobs limited to 400KB
// * Eternal free tier should cover all storage needs
// * Which replication settings to get 99.999 durability ? (already replicated in the aws region)

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

const (
  uuid_col = "Uuid"
  type_col = "BlobType"
  blob_col = "BlobProto"
  describe_retry_millis = 2000
)

var ErrNotFound = errors.New("key_not_found_in_dynamo")

// The subset of the dynamodb client used.
// Convenient for unittesting purposes.
type usedDynamoDbIf interface {
  CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
  DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
  GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
  PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

type dynamoMetadata struct {
  conf     *pb.Config
  codec    types.Codec
  aws_conf *aws.Config
  client   usedDynamoDbIf
  // Needed because aws sdk requires pointers to string
  uuid_col string
  type_col string
  blob_col string
  describe_retry time.Duration
}

func NewMetadata(conf *pb.Config, aws_conf *aws.Config, codec types.Codec) (*dynamoMetadata, error) { //(types.Metadata, error) {
  meta := &dynamoMetadata{
    conf: conf,
    codec: codec,
    aws_conf: aws_conf,
    client: dynamodb.NewFromConfig(*aws_conf),
    uuid_col: uuid_col,
    type_col: type_col,
    blob_col: blob_col,
    describe_retry: describe_retry_millis * time.Millisecond,
  }
  return meta, nil
}

func wrapInChan(err error) (<-chan error) {
  done := make(chan error, 1)
  done <- err
  close(done)
  return done
}

func (self *dynamoMetadata) describeTable(ctx context.Context, tabname string) (*dyn_types.TableDescription, error) {
  params := &dynamodb.DescribeTableInput{
    TableName: &self.conf.Aws.DynamoTableName,
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
  tabname := self.conf.Aws.DynamoTableName

  attrs := []dyn_types.AttributeDefinition{
    dyn_types.AttributeDefinition{&self.uuid_col, dyn_types.ScalarAttributeTypeS},
    dyn_types.AttributeDefinition{&self.type_col, dyn_types.ScalarAttributeTypeS},
  }
  schema := []dyn_types.KeySchemaElement{
    dyn_types.KeySchemaElement{&self.uuid_col, dyn_types.KeyTypeHash},
    dyn_types.KeySchemaElement{&self.type_col, dyn_types.KeyTypeRange},
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
      return wrapInChan(nil)
    }
    return wrapInChan(err)
  }
  if result.TableDescription.TableStatus == dyn_types.TableStatusActive {
    return wrapInChan(nil)
  }
  return self.waitForTableCreation(ctx, tabname)
}

func getItemKey(key string, msg proto.Message) map[string]dyn_types.AttributeValue {
  typename := msg.ProtoReflect().Descriptor().FullName()
  composite_k := map[string]dyn_types.AttributeValue{
    uuid_col: &dyn_types.AttributeValueMemberS{key},
    type_col: &dyn_types.AttributeValueMemberS{string(typename)},
  }
  return composite_k
}

// Returns ErrNotFound if the item does not exist in the database.
func (self *dynamoMetadata) ReadObject(ctx context.Context, key string, msg proto.Message) error {
  consistent := false
  params := &dynamodb.GetItemInput{
    TableName: &self.conf.Aws.DynamoTableName,
    Key: getItemKey(key, msg),
    ProjectionExpression: &self.blob_col,
    ConsistentRead: &consistent,
    ReturnConsumedCapacity: dyn_types.ReturnConsumedCapacityNone,
  }
  result, err := self.client.GetItem(ctx, params)
  if err != nil { return err }
  abstract_val, found := result.Item[self.blob_col]
  if !found { return ErrNotFound }

  switch v := abstract_val.(type) {
    case *dyn_types.AttributeValueMemberB:
      err = proto.Unmarshal(v.Value, msg)
   default:
     return fmt.Errorf("Malformed value for '%v': %v", key, v)
  }
  return err
}

// Create or updates
func (self *dynamoMetadata) WriteObject(ctx context.Context, key string, msg proto.Message) error {
  var err error
  var blob []byte
  item := getItemKey(key, msg)
  blob, err = proto.Marshal(msg)
  if err != nil { return err }
  item[blob_col] = &dyn_types.AttributeValueMemberB{blob}
  params := &dynamodb.PutItemInput{
    TableName: &self.conf.Aws.DynamoTableName,
    Item: item,
  }
  _, err = self.client.PutItem(ctx, params)
  return err
}

func (self *dynamoMetadata) RecordSnapshotSeqHead(ctx context.Context, new_seq *pb.SnapshotSequence) (*pb.SnapshotSeqHead, error) {
  head := &pb.SnapshotSeqHead{}
  err := self.ReadObject(ctx, new_seq.Volume.Uuid, head)
  is_new_head := errors.Is(err, ErrNotFound)
  if err != nil && !is_new_head { return nil, err }

  if head.CurSeqUuid == new_seq.Uuid {
    util.PbInfof("Noop already current seq in head: %v", head)
    return head, nil
  }
  if is_new_head { head.Uuid = new_seq.Volume.Uuid }
  if !is_new_head { head.PrevSeqUuid = append(head.PrevSeqUuid, head.CurSeqUuid) }
  head.CurSeqUuid = new_seq.Uuid

  err = ValidateSnapshotSequenceBeforeAddingToHead(head, new_seq)
  if err != nil { return nil, err }

  err = self.WriteObject(ctx, new_seq.Volume.Uuid, head)
  if err != nil { return nil, err }
  util.PbInfof("Wrote head: %v", head)
  return head, nil
}

