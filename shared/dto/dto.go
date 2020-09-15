package dto

import (
	"github.com/labstack/echo/v4"
)

type (
	AppContext struct {
		echo.Context
		MandatoryRequestDto
	}

	ErrorCode struct {
		Code                 string
		Message              string
		WrappedError         error
		FrontEndErrorMessage string
	}

	BaseResponseDto struct {
		Code       string      `json:"code"`
		Message    string      `json:"message"`
		Data       interface{} `json:"data"`
		Errors     []string    `json:"errors"`
		ServerTime int64       `json:"serverTime"`
	}

	MandatoryRequestDto struct {
		Username          string `json:"username" validate:"required"`
		Language          string `json:"lang,omitempty"`
		Login             int    `json:"login,omitempty"`
		CustomerUserAgent string `json:"customerUserAgent,omitempty"`
		CustomerIPAddress string `json:"customerIpAddress,omitempty"`
		Currency          string `json:"currency,omitempty"`
	}
)

func (e *ErrorCode) Error() string {
	return e.Message
}
