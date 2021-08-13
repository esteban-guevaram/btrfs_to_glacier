TAB_PREFIX="Metadata_Test"
TAB_NAME="${TAB_PREFIX}_coucou2"
PROTO_INCLUDE="src/proto"
PROTO_DESC="${PROTO_INCLUDE}/messages.proto"
PB_TYPE="messages.SubVolume"

create_table() {
  aws dynamodb create-table \
    --table-name="$TAB_NAME" \
    --attribute-definitions AttributeName=Uuid,AttributeType=S AttributeName=BlobType,AttributeType=S \
    --key-schema AttributeName=Uuid,KeyType=HASH AttributeName=BlobType,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST
  
  while true; do
    sleep 2
    aws dynamodb describe-table \
      --table-name="$TAB_NAME" \
      | grep '"TableStatus": "ACTIVE"' \
      && return
  done
}

clean_tables() {
  local -a to_delete=( `aws dynamodb list-tables | grep "$TAB_PREFIX"  | sed -r 's/ |"//g'` )
  for tabname in "${to_delete[@]}"; do
    aws dynamodb delete-table \
      --table-name="$tabname"
  done
}

add_item() {
  local proto_bin="/tmp/dynamo_proto_write"
  local key="`uuidgen`"
  local test_pb="
    uuid: '$key'
    mounted_path: '/tmp/coucou'
    created_ts: `date +%s`
  "
  echo -n "$test_pb" | protoc -I"$PROTO_INCLUDE" --encode="$PB_TYPE" "$PROTO_DESC" \
    | base64 -w 0 > "$proto_bin"

  aws dynamodb put-item \
    --table-name="$TAB_NAME" \
    --item='{ "Uuid": { "S": "'$key'" }, "BlobType": { "S": "'$PB_TYPE'"}, "BlobProto": { "B": "'"`cat $proto_bin`"'"}}'
}

read_item() {
  local proto_bin="/tmp/dynamo_proto_read"
  local key_file="/tmp/dynamo_key"

  aws dynamodb scan \
    --table-name="$TAB_NAME" \
    --filter-expression="BlobType = :type" \
    --projection-expression="#Uuid" \
    --expression-attribute-names='{ "#Uuid": "Uuid" }' \
    --expression-attribute-values='{ ":type": { "S": "'$PB_TYPE'" }}' \
    --max-items=1 \
    | grep '"S":' | sed -r 's/.*"([^"]+)"$/\1/g' \
    > "$key_file"

  aws dynamodb get-item \
    --table-name="$TAB_NAME" \
    --key='{ "Uuid": { "S": "'"`cat $key_file`"'" }, "BlobType": { "S": "'$PB_TYPE'"}}' \
    | grep '"B":' | sed -r 's/.*"([^"]+)"$/\1/g' \
    > "$proto_bin"

  cat "$proto_bin" | base64 -d | base64 -d \
    | protoc -I"$PROTO_INCLUDE" --decode="$PB_TYPE" "$PROTO_DESC"
}

main() {
 #clean_tables
 #create_table
 add_item
 read_item
}
main

