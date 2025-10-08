package awsiam

import (
	"errors"
	"testing"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-test/deep"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
)

var (
	testSink = lagertest.NewTestSink()
	iamPath  = "/"
	userName = "iam-user"
)

func TestMain(m *testing.M) {
	logger.RegisterSink(testSink)
}

func TestDescribeIAMUser(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
	}, logger)

	userDetails, err := iamUserClient.Describe(userName)
	if err != nil {
		t.Fatal(err)
	}

	properUserDetails := UserDetails{
		UserName: userName,
		UserARN:  "user-arn",
		UserID:   "user-id",
	}
	if diff := deep.Equal(userDetails, properUserDetails); diff != nil {
		t.Error(diff)
	}
}

func TestDescribeIAMUserError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
		getUserErr: errors.New("operation failed"),
	}, logger)

	_, err := iamUserClient.Describe(userName)
	if err == nil {
		t.Fatal("expected error but received none")
	}

	if err.Error() != "operation failed" {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDescribeIAMUserAWSError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		getUserOutput: &iam.GetUserOutput{
			User: &iamTypes.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			},
		},
		getUserErr: &types.AccessDeniedException{
			Message: aws.String("access denied"),
		},
	}, logger)

	_, err := iamUserClient.Describe(userName)

	var accessDeniedException *types.AccessDeniedException
	if !errors.As(err, &accessDeniedException) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCreateUser(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		createUserOutput: &iam.CreateUserOutput{
			User: &iamTypes.User{
				Arn: aws.String("user-arn"),
			},
		},
	}, logger)

	iamTags := []iamTypes.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}

	userArn, err := iamUserClient.Create(userName, iamPath, iamTags)
	if err != nil {
		t.Fatal(err)
	}

	if userArn != "user-arn" {
		t.Fatalf("unexpected user ARN: %s", userArn)
	}
}

func TestCreateUserError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:      userName,
		createUserErr: &iamTypes.EntityAlreadyExistsException{},
	}, logger)

	iamTags := []iamTypes.Tag{
		{
			Key:   aws.String("foo"),
			Value: aws.String("bar"),
		},
	}

	_, err := iamUserClient.Create(userName, iamPath, iamTags)

	var accessDeniedException *iamTypes.EntityAlreadyExistsException
	if !errors.As(err, &accessDeniedException) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDeleteUser(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{}, logger)

	err := iamUserClient.Delete(userName)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteUserError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		deleteUserErr: &iamTypes.DeleteConflictException{},
	}, logger)

	err := iamUserClient.Delete(userName)
	var deleteConflictException *iamTypes.DeleteConflictException
	if !errors.As(err, &deleteConflictException) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestListAccessKeys(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		listAccessKeysOutput: iam.ListAccessKeysOutput{
			AccessKeyMetadata: []iamTypes.AccessKeyMetadata{
				{
					AccessKeyId: aws.String("access-key-id-1"),
				},
				{
					AccessKeyId: aws.String("access-key-id-2"),
				},
			},
		},
	}, logger)

	accessKeys, err := iamUserClient.ListAccessKeys(userName)
	if err != nil {
		t.Fatal(err)
	}

	if diff := deep.Equal(accessKeys, []string{"access-key-id-1", "access-key-id-2"}); diff != nil {
		t.Error(diff)
	}
}

func TestListAccessKeysError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:          userName,
		listAccessKeysErr: &iamTypes.NoSuchEntityException{},
	}, logger)

	_, err := iamUserClient.ListAccessKeys(userName)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCreateAccessKey(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
		createAccessKeyOutput: iam.CreateAccessKeyOutput{
			AccessKey: &iamTypes.AccessKey{
				AccessKeyId:     aws.String("access-key-id"),
				SecretAccessKey: aws.String("secret-access-key"),
			},
		},
	}, logger)

	accessKeyID, secretAccessKey, err := iamUserClient.CreateAccessKey(userName)
	if err != nil {
		t.Fatal(err)
	}

	if accessKeyID != "access-key-id" {
		t.Fatalf("unexpected access key: %s", accessKeyID)
	}

	if secretAccessKey != "secret-access-key" {
		t.Fatalf("unexpected secret key: %s", secretAccessKey)
	}
}

func TestCreateAccessKeyError(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName:           userName,
		createAccessKeyErr: &iamTypes.NoSuchEntityException{},
	}, logger)

	_, _, err := iamUserClient.CreateAccessKey(userName)
	var exception *iamTypes.NoSuchEntityException
	if !errors.As(err, &exception) {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestDeleteAccessKey(t *testing.T) {
	iamUserClient := NewIAMUserClient(&MockIAMClient{
		userName: userName,
	}, logger)
	err := iamUserClient.DeleteAccessKey(userName, "access-key-id")
	if err != nil {
		t.Fatal(err)
	}
}

// var _ = Describe("DeleteAccessKey", func() {
// 	var (
// 		accessKeyID string

// 		deleteAccessKeyInput *iam.DeleteAccessKeyInput
// 		deleteAccessKeyError error
// 	)

// 	BeforeEach(func() {
// 		accessKeyID = "access-key-id"

// 		deleteAccessKeyInput = &iam.DeleteAccessKeyInput{
// 			UserName:    aws.String(userName),
// 			AccessKeyId: aws.String(accessKeyID),
// 		}
// 		deleteAccessKeyError = nil
// 	})

// 	JustBeforeEach(func() {
// 		iamsvc.Handlers.Clear()

// 		iamCall = func(r *request.Request) {
// 			Expect(r.Operation.Name).To(Equal("DeleteAccessKey"))
// 			Expect(r.Params).To(BeAssignableToTypeOf(&iam.DeleteAccessKeyInput{}))
// 			Expect(r.Params).To(Equal(deleteAccessKeyInput))
// 			r.Error = deleteAccessKeyError
// 		}
// 		iamsvc.Handlers.Send.PushBack(iamCall)
// 	})

// 	It("deletes the Access Key", func() {
// 		err := iamUserClient.DeleteAccessKey(userName, accessKeyID)
// 		Expect(err).ToNot(HaveOccurred())
// 	})

// 	Context("when deleting the Access Key fails", func() {
// 		BeforeEach(func() {
// 			deleteAccessKeyError = errors.New("operation failed")
// 		})

// 		It("returns the proper error", func() {
// 			err := iamUserClient.DeleteAccessKey(userName, accessKeyID)
// 			Expect(err).To(HaveOccurred())
// 			Expect(err.Error()).To(Equal("operation failed"))
// 		})

// 		Context("and it is an AWS error", func() {
// 			BeforeEach(func() {
// 				deleteAccessKeyError = awserr.New("code", "message", errors.New("operation failed"))
// 			})

// 			It("returns the proper error", func() {
// 				err := iamUserClient.DeleteAccessKey(userName, accessKeyID)
// 				Expect(err).To(HaveOccurred())
// 				Expect(err.Error()).To(Equal("code: message"))
// 			})
// 		})
// 	})
// })

// var _ = Describe("ListAttachedUserPolicies", func() {
// 	var (
// 		listAttachedUserPoliciesAttachedPolicies []*iam.AttachedPolicy

// 		listAttachedUserPoliciesInput *iam.ListAttachedUserPoliciesInput
// 		listAttachedUserPoliciesError error
// 	)

// 	BeforeEach(func() {
// 		listAttachedUserPoliciesAttachedPolicies = []*iam.AttachedPolicy{
// 			{
// 				PolicyArn: aws.String("user-policy-1"),
// 			},
// 			{
// 				PolicyArn: aws.String("user-policy-2"),
// 			},
// 		}

// 		listAttachedUserPoliciesInput = &iam.ListAttachedUserPoliciesInput{
// 			UserName:   aws.String(userName),
// 			PathPrefix: aws.String(iamPath),
// 		}
// 		listAttachedUserPoliciesError = nil
// 	})

// 	JustBeforeEach(func() {
// 		iamsvc.Handlers.Clear()

// 		iamCall = func(r *request.Request) {
// 			Expect(r.Operation.Name).To(Equal("ListAttachedUserPolicies"))
// 			Expect(r.Params).To(BeAssignableToTypeOf(&iam.ListAttachedUserPoliciesInput{}))
// 			Expect(r.Params).To(Equal(listAttachedUserPoliciesInput))
// 			data := r.Data.(*iam.ListAttachedUserPoliciesOutput)
// 			data.AttachedPolicies = listAttachedUserPoliciesAttachedPolicies
// 			r.Error = listAttachedUserPoliciesError
// 		}
// 		iamsvc.Handlers.Send.PushBack(iamCall)
// 	})

// 	It("lists the Attached User Policies", func() {
// 		attachedUserPolicies, err := iamUserClient.ListAttachedUserPolicies(userName, iamPath)
// 		Expect(err).ToNot(HaveOccurred())
// 		Expect(attachedUserPolicies).To(Equal([]string{"user-policy-1", "user-policy-2"}))
// 	})

// 	Context("when listing the Attached User Policies fails", func() {
// 		BeforeEach(func() {
// 			listAttachedUserPoliciesError = errors.New("operation failed")
// 		})

// 		It("returns the proper error", func() {
// 			_, err := iamUserClient.ListAttachedUserPolicies(userName, iamPath)
// 			Expect(err).To(HaveOccurred())
// 			Expect(err.Error()).To(Equal("operation failed"))
// 		})

// 		Context("and it is an AWS error", func() {
// 			BeforeEach(func() {
// 				listAttachedUserPoliciesError = awserr.New("code", "message", errors.New("operation failed"))
// 			})

// 			It("returns the proper error", func() {
// 				_, err := iamUserClient.ListAttachedUserPolicies(userName, iamPath)
// 				Expect(err).To(HaveOccurred())
// 				Expect(err.Error()).To(Equal("code: message"))
// 			})
// 		})
// 	})
// })

// var _ = Describe("AttachUserPolicy", func() {
// 	var (
// 		policyARN string

// 		attachUserPolicyInput *iam.AttachUserPolicyInput
// 		attachUserPolicyError error
// 	)

// 	BeforeEach(func() {
// 		policyARN = "policy-arn"

// 		attachUserPolicyInput = &iam.AttachUserPolicyInput{
// 			PolicyArn: aws.String(policyARN),
// 			UserName:  aws.String(userName),
// 		}
// 		attachUserPolicyError = nil
// 	})

// 	JustBeforeEach(func() {
// 		iamsvc.Handlers.Clear()

// 		iamCall = func(r *request.Request) {
// 			Expect(r.Operation.Name).To(Equal("AttachUserPolicy"))
// 			Expect(r.Params).To(BeAssignableToTypeOf(&iam.AttachUserPolicyInput{}))
// 			Expect(r.Params).To(Equal(attachUserPolicyInput))
// 			r.Error = attachUserPolicyError
// 		}
// 		iamsvc.Handlers.Send.PushBack(iamCall)
// 	})

// 	It("attaches the Policy to the User", func() {
// 		err := iamUserClient.AttachUserPolicy(userName, policyARN)
// 		Expect(err).ToNot(HaveOccurred())
// 	})

// 	Context("when attaching the Policy to the User fails", func() {
// 		BeforeEach(func() {
// 			attachUserPolicyError = errors.New("operation failed")
// 		})

// 		It("returns the proper error", func() {
// 			err := iamUserClient.AttachUserPolicy(userName, policyARN)
// 			Expect(err).To(HaveOccurred())
// 			Expect(err.Error()).To(Equal("operation failed"))
// 		})

// 		Context("and it is an AWS error", func() {
// 			BeforeEach(func() {
// 				attachUserPolicyError = awserr.New("code", "message", errors.New("operation failed"))
// 			})

// 			It("returns the proper error", func() {
// 				err := iamUserClient.AttachUserPolicy(userName, policyARN)
// 				Expect(err).To(HaveOccurred())
// 				Expect(err.Error()).To(Equal("code: message"))
// 			})
// 		})
// 	})
// })

// var _ = Describe("DetachUserPolicy", func() {
// 	var (
// 		policyARN string

// 		detachUserPolicyInput *iam.DetachUserPolicyInput
// 		detachUserPolicyError error
// 	)

// 	BeforeEach(func() {
// 		policyARN = "policy-arn"

// 		detachUserPolicyInput = &iam.DetachUserPolicyInput{
// 			PolicyArn: aws.String(policyARN),
// 			UserName:  aws.String(userName),
// 		}
// 		detachUserPolicyError = nil
// 	})

// 	JustBeforeEach(func() {
// 		iamsvc.Handlers.Clear()

// 		iamCall = func(r *request.Request) {
// 			Expect(r.Operation.Name).To(Equal("DetachUserPolicy"))
// 			Expect(r.Params).To(BeAssignableToTypeOf(&iam.DetachUserPolicyInput{}))
// 			Expect(r.Params).To(Equal(detachUserPolicyInput))
// 			r.Error = detachUserPolicyError
// 		}
// 		iamsvc.Handlers.Send.PushBack(iamCall)
// 	})

// 	It("detaches the Policy from the User", func() {
// 		err := iamUserClient.DetachUserPolicy(userName, policyARN)
// 		Expect(err).ToNot(HaveOccurred())
// 	})

// 	Context("when detaching the Policy from the User fails", func() {
// 		BeforeEach(func() {
// 			detachUserPolicyError = errors.New("operation failed")
// 		})

// 		It("returns the proper error", func() {
// 			err := iamUserClient.DetachUserPolicy(userName, policyARN)
// 			Expect(err).To(HaveOccurred())
// 			Expect(err.Error()).To(Equal("operation failed"))
// 		})

// 		Context("and it is an AWS error", func() {
// 			BeforeEach(func() {
// 				detachUserPolicyError = awserr.New("code", "message", errors.New("operation failed"))
// 			})

// 			It("returns the proper error", func() {
// 				err := iamUserClient.DetachUserPolicy(userName, policyARN)
// 				Expect(err).To(HaveOccurred())
// 				Expect(err.Error()).To(Equal("code: message"))
// 			})
// 		})
// 	})
// })
// })
