package elasticsearch

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/opensearchservice"
)

func TestIsInvalidTypeException(t *testing.T) {
	isInvalidType := isInvalidTypeException(&opensearchservice.InvalidTypeException{})
	if !isInvalidType {
		t.Fatal("expected isInvalidTypeException() to return true")
	}
}
