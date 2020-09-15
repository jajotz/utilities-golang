package util

import (
	"fmt"
)

type (
	Ping interface {
		Ping() error
	}
	AppError struct {
		Code           string
		Message        string
		HttpStatusCode int
	}
)

func (v *AppError) Error() string {
	return v.Message
}

func NewAppError(code, message string, status int) error {
	return &AppError{
		Code:           code,
		Message:        message,
		HttpStatusCode: status,
	}
}

func ErrorStackTrace(err error) string {
	return fmt.Sprintf("%+v\n", err)
}
