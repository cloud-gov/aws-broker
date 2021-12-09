package response

import "net/http"

// Response represents the common data for all types of responses to have and implement.
type Response interface {
	GetStatusCode() int
	GetResponseType() Type
}

type baseResponse struct {
	StatusCode int  `json:"-"`
	StatusType Type `json:"-"`
}

func (resp *baseResponse) GetStatusCode() int {
	return resp.StatusCode
}

func (resp *baseResponse) GetResponseType() Type {
	return resp.StatusType
}

type genericResponse struct {
	baseResponse
	Description string `json:"description"`
}

type asyncOperationResponse struct {
	baseResponse
	Operation string `json:"operation"`
}

type lastOperationResponse struct {
	baseResponse
	State       string `json:"state"`
	Description string `json:"description"`
}

// Type indicates the type of response. Nice for debug situations.
type Type string

// These contain the list of response types. Useful for debug situations.
var (
	// SuccessCreateResponseType represents a response for a successful instance creation.
	SuccessCreateResponseType Type = "success_create"
	// SuccessAcceptedResponseType represents a response for a successful instance creation.
	SuccessAcceptedResponseType Type = "success_accept"
	// SuccessLastOperationResponseType represents a response for a successful last operation.
	SuccessLastOperationResponseType Type = "success_lastoperation"
	// SuccessBindResponseType represents a response for a successful instance binding.
	SuccessBindResponseType Type = "success_bind"
	// SuccessDeleteResponseType represents a response for a successful instance deletion.
	SuccessDeleteResponseType Type = "success_delete"
	// ErrorResponseType represents a response for an error.
	ErrorResponseType Type = "error"
)

// NewErrorResponse is the constructor for an errorResponse.
func NewErrorResponse(statusCode int, description string) Response {
	return &genericResponse{baseResponse: baseResponse{StatusCode: statusCode, StatusType: ErrorResponseType}, Description: description}
}

// NewSuccessResponse is the constructor for an SuccessResponse.
func newSuccessResponse(statusCode int, responseType Type, description string) Response {
	return &genericResponse{baseResponse: baseResponse{StatusCode: statusCode, StatusType: responseType}, Description: description}
}

var (
	// ErrNoRequestBodyResponse is a response indicating there was no request body.
	ErrNoRequestBodyResponse = NewErrorResponse(http.StatusBadRequest, "No Request Body")

	// ErrUnprocessableEntityResponse is a response indicating the service instance not async
	ErrUnprocessableEntityResponse = NewErrorResponse(http.StatusUnprocessableEntity, "This Service Instance requires client support for asynchronous binding operations")
)

type successBindResponse struct {
	baseResponse
	Credentials map[string]string `json:"credentials"` // Needed for sending credentials for service.
}

// NewSuccessBindResponse is the constructor for a successBindResponse.
func NewSuccessBindResponse(credentials map[string]string) Response {
	return &successBindResponse{baseResponse: baseResponse{StatusCode: http.StatusCreated, StatusType: SuccessBindResponseType}, Credentials: credentials}
}

// NewSuccessLastOperation for async responses
func NewSuccessLastOperation(state string, description string) Response {
	return &lastOperationResponse{baseResponse: baseResponse{StatusCode: http.StatusOK, StatusType: SuccessLastOperationResponseType}, State: state, Description: description}
}

var (
	// SuccessCreateResponse represents the response that all successful instance creations should return.
	SuccessCreateResponse = newSuccessResponse(http.StatusCreated, SuccessCreateResponseType, "The instance was created")
	// SuccessAcceptedResponse represents the response that all successful instance acceptions should return.
	SuccessAcceptedResponse = newSuccessResponse(http.StatusAccepted, SuccessAcceptedResponseType, "The operation was accepted")
	// SuccessDeleteResponse represents the response that all successful instance deletions should return.
	SuccessDeleteResponse = newSuccessResponse(http.StatusOK, SuccessDeleteResponseType, "The instance was deleted")
)

// If a broker has an async operation ( create, modify, delete, bind) and want to return an "operation" they should use this
// Otherwise they can return SuccessAcceptedResponse
func NewAsyncOperationResponse(operation string) Response {
	return &asyncOperationResponse{baseResponse: baseResponse{StatusCode: http.StatusAccepted, StatusType: SuccessAcceptedResponseType}, Operation: operation}
}
