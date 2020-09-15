package validator

import (
	v9 "gopkg.in/go-playground/validator.v9"
)

type (
	Validator interface {
		Validate(interface{}) error
		ValidateVar(interface{}, string) error
	}

	validate struct {
		instance *v9.Validate
	}
)

func New() Validator {
	return &validate{instance: v9.New()}
}

func (v *validate) Validate(object interface{}) error {
	if err := v.instance.Struct(object); err != nil {
		return err
	}

	return nil
}

func (v *validate) ValidateVar(object interface{}, constraint string) error {
	if err := v.instance.Var(object, constraint); err != nil {
		return err
	}

	return nil
}
