package validator

import (
	"testing"
)

type (
	TestValueObject struct {
		Name   string `validate:"required"`
		Height int    `validate:"required,min=1,max=200"`
	}
)

func Test_ValidateStruct_returns_success(t *testing.T) {
	vo := TestValueObject{Name: "gw", Height: 200}

	validator := New()

	if err := validator.Validate(vo); err != nil {
		t.Errorf("struct should be valid!")
	}
}

func Test_ValidateStruct_returns_fail(t *testing.T) {
	vo := TestValueObject{}

	validator := New()

	if err := validator.Validate(vo); err == nil {
		t.Errorf("struct should be invalid")
	}
}
