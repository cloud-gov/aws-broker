package errors

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

func DidAwsCallSucceed(err error) bool {
	// TODO Eventually return a formatted error object.
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
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
		return false
	}
	return true
}
