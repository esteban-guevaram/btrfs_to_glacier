package cloud

import (
  "context"
  "fmt"
  "testing"
  "time"

  pb "btrfs_to_glacier/messages"
  "btrfs_to_glacier/types"
  "btrfs_to_glacier/util"

  "github.com/aws/aws-sdk-go-v2/service/dynamodb"
  dyn_types "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

  "google.golang.org/protobuf/proto"
)

type keyAndtype struct {
  Key string
  Type string
}

type mockDynamoDbClient struct {
  Err error
  Data map[keyAndtype][]byte
  CreateTableOutput   dyn_types.TableDescription
  DescribeTableOutput dyn_types.TableDescription
}

func stringOrDie(vals map[string]dyn_types.AttributeValue, col string) string {
  switch v := vals[col].(type) {
    case *dyn_types.AttributeValueMemberS:
      return v.Value
   default:
     util.Fatalf("Malformed value for col '%v'", col)
     return ""
  }
}
func blobOrDie(vals map[string]dyn_types.AttributeValue, col string) []byte {
  switch v := vals[col].(type) {
    case *dyn_types.AttributeValueMemberB:
      return v.Value
   default:
     util.Fatalf("Malformed value for col '%v'", col)
     return nil
  }
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
func (self *mockDynamoDbClient) GetItem(
    ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
  key := keyAndtype{
    Key: stringOrDie(params.Key, uuid_col),
    Type: stringOrDie(params.Key, type_col),
  }
  data, found := self.Data[key]
  if !found {
    return &dynamodb.GetItemOutput{ Item: nil }, self.Err
  }
  item := map[string]dyn_types.AttributeValue{
    blob_col: &dyn_types.AttributeValueMemberB{data},
  }
  result := &dynamodb.GetItemOutput{ Item: item, }
  return result, self.Err
}
func (self *mockDynamoDbClient) PutItem(
    ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
  key := keyAndtype{
    Key: stringOrDie(params.Item, uuid_col),
    Type: stringOrDie(params.Item, type_col),
  }
  self.Data[key] = blobOrDie(params.Item, blob_col)
  return &dynamodb.PutItemOutput{}, nil
}
func (self *mockDynamoDbClient) putForTest(k string, p proto.Message) {
  key := keyAndtype{
    Key: k,
    Type: string(p.ProtoReflect().Descriptor().FullName()),
  }
  if b, err := proto.Marshal(p); err == nil { self.Data[key] = b }
}
func (self *mockDynamoDbClient) getForTest(k string, p proto.Message) bool {
  key := keyAndtype{
    Key: k,
    Type: string(p.ProtoReflect().Descriptor().FullName()),
  }
  if b, found := self.Data[key]; found {
    if err := proto.Unmarshal(b, p); err != nil { return false }
    return true
  }
  return false
}

func buildTestMetadata(t *testing.T) (*dynamoMetadata, *mockDynamoDbClient) {
  conf := util.LoadTestConf()
  client := new(mockDynamoDbClient)
  client.Data = make(map[keyAndtype][]byte)
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
    blob_col: blob_col,
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

func TestRecordSnapshotSeqHead_New(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, _ := buildTestMetadata(t)
  new_seq := dummySnapshotSequence("vol", "seq")
  expect_head := dummySnapshotSeqHead(new_seq)
  head, err := metadata.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, head, expect_head)
}

func TestRecordSnapshotSeqHead_Add(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestMetadata(t)
  old_seq := dummySnapshotSequence("vol", "seq1")
  new_seq := dummySnapshotSequence("vol", "seq2")
  expect_head := dummySnapshotSeqHead(old_seq)
  client.putForTest(expect_head.Uuid, expect_head)
  expect_head.CurSeqUuid = new_seq.Uuid
  expect_head.PrevSeqUuid = []string{old_seq.Uuid}

  head, err := metadata.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, head, expect_head)
}

func TestRecordSnapshotSeqHead_Noop(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestMetadata(t)
  new_seq := dummySnapshotSequence("vol", "seq")
  expect_head := dummySnapshotSeqHead(new_seq)
  client.putForTest(expect_head.Uuid, expect_head)

  head, err := metadata.RecordSnapshotSeqHead(ctx, new_seq)
  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, head, expect_head)
}

func TestAppendSnapshotToSeq_New(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestMetadata(t)
  seq := dummySnapshotSequence("vol", "seq")
  seq.SnapUuids = nil
  expect_seq := dummySnapshotSequence("vol", "seq")
  snap_to_add := dummySnapshot(expect_seq.SnapUuids[0], "vol")
  new_seq, err := metadata.AppendSnapshotToSeq(ctx, seq, snap_to_add)

  if err != nil { t.Errorf("Returned error: %v", err) }
  util.EqualsOrFailTest(t, new_seq, expect_seq)
  if !client.getForTest("seq", &pb.SnapshotSequence{}) { t.Errorf("No sequence persisted") }
}

func TestAppendSnapshotToSeq_Noop(t *testing.T) {
  ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
  defer cancel()
  metadata, client := buildTestMetadata(t)
  client.Err = fmt.Errorf("No methods on the client should have been called")
  seq := dummySnapshotSequence("vol", "seq")
  snap_to_add := dummySnapshot(seq.SnapUuids[0], "vol")
  _, err := metadata.AppendSnapshotToSeq(ctx, seq, snap_to_add)
  if err != nil { t.Errorf("Returned error: %v", err) }
}

