package cloud

import (
  "context"
  "testing"
  "time"

  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/service/dynamodb"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type typeAndBlob struct {
  typeName string
  blob []byte
}

type mockDynamoDbClient struct {
  Err error
  Data map[string]typeAndBlob
  CreateTableOutput   dyn_types.TableDescription
  DescribeTableOutput dyn_types.TableDescription
}

func (self *mockDynamoDbClient) CreateTable(
    ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
  result := &dynamodb.CreateTableOutput {
    TableDescription: &self.CreateTableOutput,
  }
  return result, self.Err
}
func (self *mockDynamoDbClient) DescribeTable(
    ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
  result := &dynamodb.DescribeTableOutput {
    Table: &self.DescribeTableOutput,
  }
  return result, self.Err
}

func buildTestMetadata(t *testing.T) (*dynamoMetadata, *mockDynamoDbClient) {
  conf := util.LoadTestConf()
  client := new(mockDynamoDbClient)
  client.Data = make(map[string]typeAndBlob)
  codec := new(types.MockCodec)
  aws_conf, err := NewAwsConfig(context.TODO(), conf)
  if err != nil { t.Fatalf("Failed aws config: %v", err) }

  meta := &dynamoMetadata{
    conf: conf,
    codec: codec,
    aws_conf: aws_conf,
    client: client,
    uuid_col: uuid_col,
    type_col: type_col,
    describe_retry: 2 * time.Millisecond,
  }
  return meta, client
}

func TestTableCreation_Immediate(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestMetadata(t)
  client.CreateTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusActive,
  }
  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestTableCreation_Immediate timeout")
  }
}

func TestTableCreation_Wait(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestMetadata(t)
  client.CreateTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusCreating,
  }
  client.DescribeTableOutput = dyn_types.TableDescription{
    TableStatus: dyn_types.TableStatusActive,
  }
  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestTableCreation_Immediate timeout")
  }
}

func TestTableCreation_Idempotent(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestMetadata(t)
  client.Err = &dyn_types.ResourceInUseException{}
  done := metadata.SetupMetadata(ctx)
  select {
    case err := <-done:
      if err != nil { t.Errorf("Returned error: %v", err) }
    case <-ctx.Done():
      t.Fatalf("TestTableCreation_Immediate timeout")
  }
}

