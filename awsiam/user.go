package awsiam

import (
	"errors"
)

type UserDetails struct {
	UserName string
	UserARN  string
	UserID   string
}

var (
	ErrUserDoesNotExist = errors.New("iam user does not exist")
)
