module btrfs_to_glacier

go 1.16

require (
	github.com/aws/aws-sdk-go-v2 v1.16.3
	github.com/aws/aws-sdk-go-v2/config v1.15.4
	github.com/aws/aws-sdk-go-v2/credentials v1.12.0
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.4.7
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.7
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.15.4
	github.com/aws/aws-sdk-go-v2/service/s3 v1.26.7
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.4
	github.com/aws/smithy-go v1.11.2
	github.com/google/uuid v1.3.0
	golang.org/x/crypto v0.0.0-20220427172511-eb4f295cb31f
	golang.org/x/sys v0.0.0-20220429233432-b5fbb4746d32
	golang.org/x/term v0.0.0-20220411215600-e5f449aeb171 // indirect
	google.golang.org/protobuf v1.28.0
)
