module btrfs_to_glacier

go 1.16

require (
	github.com/aws/aws-sdk-go-v2 v1.14.0
	github.com/aws/aws-sdk-go-v2/config v1.14.0
	github.com/aws/aws-sdk-go-v2/credentials v1.9.0
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.4.0
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.10.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.14.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.25.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.15.0
	github.com/aws/smithy-go v1.11.0
	github.com/google/uuid v1.3.0
	golang.org/x/crypto v0.0.0-20220214200702-86341886e292
	golang.org/x/sys v0.0.0-20220227234510-4e6760a101f9
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	google.golang.org/protobuf v1.27.1
)
