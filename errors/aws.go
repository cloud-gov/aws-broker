package errors

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

func LogAWSError(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		fmt.Println(awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			fmt.Println(reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
		}
	} else {
		// This case should never be hit, The SDK should alwsy return an
		// error which satisfies the awserr.Error interface.
		fmt.Println(err.Error())
	}
}
