package awsiam_test

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/cloudfoundry-community/s3-broker/awsiam"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
)

var _ = Describe("IAM User", func() {
	var (
		userName string
		iamPath  string

		awsSession *session.Session
		iamsvc     *iam.IAM
		iamCall    func(r *request.Request)

		testSink *lagertest.TestSink
		logger   lager.Logger

		user User
	)

	BeforeEach(func() {
		userName = "iam-user"
		iamPath = "/path/"
	})

	JustBeforeEach(func() {
		awsSession = session.New(nil)
		iamsvc = iam.New(awsSession)

		logger = lager.NewLogger("iamuser_test")
		testSink = lagertest.NewTestSink()
		logger.RegisterSink(testSink)

		user = NewIAMUser(iamsvc, logger)
	})

	var _ = Describe("Describe", func() {
		var (
			properUserDetails UserDetails

			getUser      *iam.User
			getUserInput *iam.GetUserInput
			getUserError error
		)

		BeforeEach(func() {
			properUserDetails = UserDetails{
				UserName: userName,
				UserARN:  "user-arn",
				UserID:   "user-id",
			}

			getUser = &iam.User{
				Arn:    aws.String("user-arn"),
				UserId: aws.String("user-id"),
			}
			getUserInput = &iam.GetUserInput{
				UserName: aws.String(userName),
			}
			getUserError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("GetUser"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.GetUserInput{}))
				Expect(r.Params).To(Equal(getUserInput))
				data := r.Data.(*iam.GetUserOutput)
				data.User = getUser
				r.Error = getUserError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("returns the proper User Details", func() {
			userDetails, err := user.Describe(userName)
			Expect(err).ToNot(HaveOccurred())
			Expect(userDetails).To(Equal(properUserDetails))
		})

		Context("when getting the User fails", func() {
			BeforeEach(func() {
				getUserError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := user.Describe(userName)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					getUserError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := user.Describe(userName)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Create", func() {
		var (
			createUserInput *iam.CreateUserInput
			createUserError error
		)

		BeforeEach(func() {
			createUserInput = &iam.CreateUserInput{
				UserName: aws.String(userName),
				Path:     aws.String(iamPath),
			}
			createUserError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreateUser"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.CreateUserInput{}))
				Expect(r.Params).To(Equal(createUserInput))
				data := r.Data.(*iam.CreateUserOutput)
				data.User = &iam.User{
					Arn: aws.String("user-arn"),
				}
				r.Error = createUserError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("creates the User", func() {
			userARN, err := user.Create(userName, iamPath)
			Expect(userARN).To(Equal("user-arn"))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when creating the User fails", func() {
			BeforeEach(func() {
				createUserError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := user.Create(userName, iamPath)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					createUserError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := user.Create(userName, iamPath)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("Delete", func() {
		var (
			deleteUserInput *iam.DeleteUserInput
			deleteUserError error
		)

		BeforeEach(func() {
			deleteUserInput = &iam.DeleteUserInput{
				UserName: aws.String(userName),
			}
			deleteUserError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DeleteUser"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.DeleteUserInput{}))
				Expect(r.Params).To(Equal(deleteUserInput))
				r.Error = deleteUserError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("deletes the User", func() {
			err := user.Delete(userName)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when deleting the User fails", func() {
			BeforeEach(func() {
				deleteUserError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := user.Delete(userName)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					deleteUserError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := user.Delete(userName)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("ListAccessKeys", func() {
		var (
			listAccessKeysMetadata []*iam.AccessKeyMetadata

			listAccessKeysInput *iam.ListAccessKeysInput
			listAccessKeysError error
		)

		BeforeEach(func() {
			listAccessKeysMetadata = []*iam.AccessKeyMetadata{
				&iam.AccessKeyMetadata{
					AccessKeyId: aws.String("access-key-id-1"),
				},
				&iam.AccessKeyMetadata{
					AccessKeyId: aws.String("access-key-id-2"),
				},
			}

			listAccessKeysInput = &iam.ListAccessKeysInput{
				UserName: aws.String(userName),
			}
			listAccessKeysError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("ListAccessKeys"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.ListAccessKeysInput{}))
				Expect(r.Params).To(Equal(listAccessKeysInput))
				data := r.Data.(*iam.ListAccessKeysOutput)
				data.AccessKeyMetadata = listAccessKeysMetadata
				r.Error = listAccessKeysError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("lists the User Access Key", func() {
			accessKeys, err := user.ListAccessKeys(userName)
			Expect(err).ToNot(HaveOccurred())
			Expect(accessKeys).To(Equal([]string{"access-key-id-1", "access-key-id-2"}))
		})

		Context("when listing the User Access Key fails", func() {
			BeforeEach(func() {
				listAccessKeysError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := user.ListAccessKeys(userName)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					listAccessKeysError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := user.ListAccessKeys(userName)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("CreateAccessKey", func() {
		var (
			createAccessKey *iam.AccessKey

			createAccessKeyInput *iam.CreateAccessKeyInput
			createAccessKeyError error
		)

		BeforeEach(func() {
			createAccessKey = &iam.AccessKey{
				UserName:        aws.String(userName),
				AccessKeyId:     aws.String("access-key-id"),
				SecretAccessKey: aws.String("secret-access-key"),
			}

			createAccessKeyInput = &iam.CreateAccessKeyInput{
				UserName: aws.String(userName),
			}
			createAccessKeyError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreateAccessKey"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.CreateAccessKeyInput{}))
				Expect(r.Params).To(Equal(createAccessKeyInput))
				data := r.Data.(*iam.CreateAccessKeyOutput)
				data.AccessKey = createAccessKey
				r.Error = createAccessKeyError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("creates the Access Key", func() {
			accessKeyID, secretAccessKey, err := user.CreateAccessKey(userName)
			Expect(err).ToNot(HaveOccurred())
			Expect(accessKeyID).To(Equal("access-key-id"))
			Expect(secretAccessKey).To(Equal("secret-access-key"))
		})

		Context("when creating the Access Key fails", func() {
			BeforeEach(func() {
				createAccessKeyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, _, err := user.CreateAccessKey(userName)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					createAccessKeyError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, _, err := user.CreateAccessKey(userName)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("DeleteAccessKey", func() {
		var (
			accessKeyID string

			deleteAccessKeyInput *iam.DeleteAccessKeyInput
			deleteAccessKeyError error
		)

		BeforeEach(func() {
			accessKeyID = "access-key-id"

			deleteAccessKeyInput = &iam.DeleteAccessKeyInput{
				UserName:    aws.String(userName),
				AccessKeyId: aws.String(accessKeyID),
			}
			deleteAccessKeyError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DeleteAccessKey"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.DeleteAccessKeyInput{}))
				Expect(r.Params).To(Equal(deleteAccessKeyInput))
				r.Error = deleteAccessKeyError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("deletes the Access Key", func() {
			err := user.DeleteAccessKey(userName, accessKeyID)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when deleting the Access Key fails", func() {
			BeforeEach(func() {
				deleteAccessKeyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := user.DeleteAccessKey(userName, accessKeyID)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					deleteAccessKeyError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := user.DeleteAccessKey(userName, accessKeyID)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("CreatePolicy", func() {
		var (
			policyName string
			template   string
			resources  []string

			createPolicy *iam.Policy

			createPolicyInput *iam.CreatePolicyInput
			createPolicyError error
		)

		BeforeEach(func() {
			policyName = "policy-name"
			template = `{
	"Version": "2012-10-17",
	"Id": "policy-name",
	"Statement": [
		{
			"Effect": "effect",
			"Action": "action",
			"Resource": {{resources "/*"}}
		}
	]
}`
			resources = []string{"resource"}

			createPolicy = &iam.Policy{
				Arn: aws.String("policy-arn"),
			}

			createPolicyInput = &iam.CreatePolicyInput{
				Path:       aws.String(iamPath),
				PolicyName: aws.String(policyName),
				PolicyDocument: aws.String(`{
	"Version": "2012-10-17",
	"Id": "policy-name",
	"Statement": [
		{
			"Effect": "effect",
			"Action": "action",
			"Resource": ["resource/*"]
		}
	]
}`),
			}
			createPolicyError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("CreatePolicy"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.CreatePolicyInput{}))
				Expect(*r.Params.(*iam.CreatePolicyInput).PolicyDocument).To(MatchJSON(*createPolicyInput.PolicyDocument))
				data := r.Data.(*iam.CreatePolicyOutput)
				data.Policy = createPolicy
				r.Error = createPolicyError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("creates the Access Key", func() {
			policyARN, err := user.CreatePolicy(policyName, iamPath, template, resources)
			Expect(err).ToNot(HaveOccurred())
			Expect(policyARN).To(Equal("policy-arn"))
		})

		Context("when creating the Policy fails", func() {
			BeforeEach(func() {
				createPolicyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := user.CreatePolicy(policyName, iamPath, template, resources)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					createPolicyError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := user.CreatePolicy(policyName, iamPath, template, resources)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("DeletePolicy", func() {
		var (
			policyARN string

			deletePolicyInput *iam.DeletePolicyInput
			deletePolicyError error
		)

		BeforeEach(func() {
			policyARN = "policy-arn"

			deletePolicyInput = &iam.DeletePolicyInput{
				PolicyArn: aws.String(policyARN),
			}
			deletePolicyError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DeletePolicy"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.DeletePolicyInput{}))
				Expect(r.Params).To(Equal(deletePolicyInput))
				r.Error = deletePolicyError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("deletes the Policy", func() {
			err := user.DeletePolicy(policyARN)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when deleting the Policy fails", func() {
			BeforeEach(func() {
				deletePolicyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := user.DeletePolicy(policyARN)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					deletePolicyError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := user.DeletePolicy(policyARN)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("ListAttachedUserPolicies", func() {
		var (
			listAttachedUserPoliciesAttachedPolicies []*iam.AttachedPolicy

			listAttachedUserPoliciesInput *iam.ListAttachedUserPoliciesInput
			listAttachedUserPoliciesError error
		)

		BeforeEach(func() {
			listAttachedUserPoliciesAttachedPolicies = []*iam.AttachedPolicy{
				&iam.AttachedPolicy{
					PolicyArn: aws.String("user-policy-1"),
				},
				&iam.AttachedPolicy{
					PolicyArn: aws.String("user-policy-2"),
				},
			}

			listAttachedUserPoliciesInput = &iam.ListAttachedUserPoliciesInput{
				UserName:   aws.String(userName),
				PathPrefix: aws.String(iamPath),
			}
			listAttachedUserPoliciesError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("ListAttachedUserPolicies"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.ListAttachedUserPoliciesInput{}))
				Expect(r.Params).To(Equal(listAttachedUserPoliciesInput))
				data := r.Data.(*iam.ListAttachedUserPoliciesOutput)
				data.AttachedPolicies = listAttachedUserPoliciesAttachedPolicies
				r.Error = listAttachedUserPoliciesError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("lists the Attached User Policies", func() {
			attachedUserPolicies, err := user.ListAttachedUserPolicies(userName, iamPath)
			Expect(err).ToNot(HaveOccurred())
			Expect(attachedUserPolicies).To(Equal([]string{"user-policy-1", "user-policy-2"}))
		})

		Context("when listing the Attached User Policies fails", func() {
			BeforeEach(func() {
				listAttachedUserPoliciesError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				_, err := user.ListAttachedUserPolicies(userName, iamPath)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					listAttachedUserPoliciesError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					_, err := user.ListAttachedUserPolicies(userName, iamPath)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("AttachUserPolicy", func() {
		var (
			policyARN string

			attachUserPolicyInput *iam.AttachUserPolicyInput
			attachUserPolicyError error
		)

		BeforeEach(func() {
			policyARN = "policy-arn"

			attachUserPolicyInput = &iam.AttachUserPolicyInput{
				PolicyArn: aws.String(policyARN),
				UserName:  aws.String(userName),
			}
			attachUserPolicyError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("AttachUserPolicy"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.AttachUserPolicyInput{}))
				Expect(r.Params).To(Equal(attachUserPolicyInput))
				r.Error = attachUserPolicyError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("attaches the Policy to the User", func() {
			err := user.AttachUserPolicy(userName, policyARN)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when attaching the Policy to the User fails", func() {
			BeforeEach(func() {
				attachUserPolicyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := user.AttachUserPolicy(userName, policyARN)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					attachUserPolicyError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := user.AttachUserPolicy(userName, policyARN)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})

	var _ = Describe("DetachUserPolicy", func() {
		var (
			policyARN string

			detachUserPolicyInput *iam.DetachUserPolicyInput
			detachUserPolicyError error
		)

		BeforeEach(func() {
			policyARN = "policy-arn"

			detachUserPolicyInput = &iam.DetachUserPolicyInput{
				PolicyArn: aws.String(policyARN),
				UserName:  aws.String(userName),
			}
			detachUserPolicyError = nil
		})

		JustBeforeEach(func() {
			iamsvc.Handlers.Clear()

			iamCall = func(r *request.Request) {
				Expect(r.Operation.Name).To(Equal("DetachUserPolicy"))
				Expect(r.Params).To(BeAssignableToTypeOf(&iam.DetachUserPolicyInput{}))
				Expect(r.Params).To(Equal(detachUserPolicyInput))
				r.Error = detachUserPolicyError
			}
			iamsvc.Handlers.Send.PushBack(iamCall)
		})

		It("detaches the Policy from the User", func() {
			err := user.DetachUserPolicy(userName, policyARN)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("when detaching the Policy from the User fails", func() {
			BeforeEach(func() {
				detachUserPolicyError = errors.New("operation failed")
			})

			It("returns the proper error", func() {
				err := user.DetachUserPolicy(userName, policyARN)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("operation failed"))
			})

			Context("and it is an AWS error", func() {
				BeforeEach(func() {
					detachUserPolicyError = awserr.New("code", "message", errors.New("operation failed"))
				})

				It("returns the proper error", func() {
					err := user.DetachUserPolicy(userName, policyARN)
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(Equal("code: message"))
				})
			})
		})
	})
})
