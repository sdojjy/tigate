package apperror

import (
	"fmt"

	"github.com/pingcap/errors"
)

type ErrorType int

const (
	// ErrorTypeUnknown is the default error type.
	ErrorTypeUnknown ErrorType = 0

	ErrorTypeEpochMismatch  ErrorType = 1
	ErrorTypeEpochSmaller   ErrorType = 2
	ErrorTypeTaskIDMismatch ErrorType = 3

	ErrorTypeInvalid    ErrorType = 101
	ErrorTypeIncomplete ErrorType = 102
	ErrorTypeDecodeData ErrorType = 103
	ErrorTypeBufferFull ErrorType = 104
	ErrorTypeDuplicate  ErrorType = 105
	ErrorTypeNotExist   ErrorType = 106
	ErrorTypeClosed     ErrorType = 107

	ErrorTypeConnectionFailed     ErrorType = 201
	ErrorTypeConnectionNotFound   ErrorType = 202
	ErrorTypeMessageCongested     ErrorType = 204
	ErrorTypeMessageReceiveFailed ErrorType = 205
	ErrorTypeMessageSendFailed    ErrorType = 206
	ErrorTypeTargetNotFound       ErrorType = 207
	ErrorTypeInvalidMessage       ErrorType = 208

	//ErrorTypeCreateEventDispatcherManagerFailed ErrorType = 300

	ErrorInvalidDDLEvent ErrorType = 301
)

func (t ErrorType) String() string {
	switch t {
	case ErrorTypeUnknown:
		return "Unknown"
	case ErrorTypeEpochMismatch:
		return "EpochMismatch"
	case ErrorTypeEpochSmaller:
		return "EpochSmaller"
	case ErrorTypeTaskIDMismatch:
		return "TaskIDMismatch"
	case ErrorTypeInvalid:
		return "Invalid"
	case ErrorTypeIncomplete:
		return "Incomplete"
	case ErrorTypeDecodeData:
		return "DecodeData"
	case ErrorTypeBufferFull:
		return "BufferFull"
	case ErrorTypeDuplicate:
		return "Duplicate"
	case ErrorTypeNotExist:
		return "NotExist"
	case ErrorTypeClosed:
		return "Closed"
	case ErrorTypeConnectionFailed:
		return "ConnectionFailed"
	case ErrorTypeConnectionNotFound:
		return "ConnectionNotFound"
	case ErrorTypeMessageCongested:
		return "MessageCongested"
	case ErrorTypeMessageReceiveFailed:
		return "MessageReceiveFailed"
	case ErrorTypeMessageSendFailed:
		return "MessageSendFailed"
	default:
		return "Unknown"
	}
}

type AppError struct {
	Type   ErrorType
	Reason string
}

func NewAppErrorS(t ErrorType) *AppError {
	return &AppError{
		Type:   t,
		Reason: "",
	}
}

func NewAppError(t ErrorType, reason string) *AppError {
	return &AppError{
		Type:   t,
		Reason: reason,
	}
}

func (e AppError) Error() string {
	return fmt.Sprintf("ErrorType: %s, Reason: %s", e.Type, e.Reason)
}

func (e AppError) GetType() ErrorType {
	return e.Type
}

func (e AppError) Equal(err AppError) bool {
	return e.Type == err.Type
}

var (
	// kv related errors
	ErrCreateEventDispatcherManagerFailed = errors.Normalize(
		"Create Event Dispatcher Manager Failed, %s",
		errors.RFCCodeText("CDC:ErrCreateEventDispatcherManagerFailed"),
	)
)
