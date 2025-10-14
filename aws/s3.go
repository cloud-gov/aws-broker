package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3ClientInterface interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

var s3ServerSideEncryptionMap = map[string]s3Types.ServerSideEncryption{
	"AES256": s3Types.ServerSideEncryptionAes256,
}

func GetS3ServerSideEncryptionEnum(encryptionString string) (*s3Types.ServerSideEncryption, error) {
	if serverSideEncryption, ok := s3ServerSideEncryptionMap[encryptionString]; ok {
		return &serverSideEncryption, nil
	}
	return nil, fmt.Errorf("invalid instance type: %s", encryptionString)
}
