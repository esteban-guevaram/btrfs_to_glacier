module btrfs_to_glacier

go 1.16

require (
	github.com/aws/aws-sdk-go-v2 v1.9.0
	github.com/aws/aws-sdk-go-v2/config v1.6.1
	github.com/aws/aws-sdk-go-v2/credentials v1.3.3
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.2.2
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.4.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.5.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.13.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.6.2
	github.com/aws/smithy-go v1.8.0
	github.com/google/uuid v1.3.0
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2
	google.golang.org/protobuf v1.27.1
)
