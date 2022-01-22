module btrfs_to_glacier

go 1.16

require (
	github.com/aws/aws-sdk-go-v2 v1.13.0
	github.com/aws/aws-sdk-go-v2/config v1.13.0
	github.com/aws/aws-sdk-go-v2/credentials v1.8.0
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.3.7
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.9.0
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.13.0
	github.com/aws/aws-sdk-go-v2/service/s3 v1.24.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.14.0
	github.com/aws/smithy-go v1.10.0
	github.com/google/uuid v1.3.0
	golang.org/x/crypto v0.0.0-20220112180741-5e0467b6c7ce
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	google.golang.org/protobuf v1.27.1
)
