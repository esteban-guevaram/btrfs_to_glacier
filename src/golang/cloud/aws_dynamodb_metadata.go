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
)

const (
  uuid_col = "Uuid"
  type_col = "BlobType"
  describe_retry_millis = 2000
)

// The subset of the dynamodb client used.
// Convenient for unittesting purposes.
type usedDynamoDbIf interface {
  CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
  DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

type dynamoMetadata struct {
  conf     *pb.Config
  codec    types.Codec
  aws_conf *aws.Config
  client   usedDynamoDbIf
  // Needed because aws sdk requires pointers to string
  uuid_col string
  type_col string
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

