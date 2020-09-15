package config

import (
	"testing"
)

var (
	dev = "./development.properties"
)

type teststruct struct {
	Secret string `mapstructure:"secret"`
}

func Test_New_ok(t *testing.T) {
	object := teststruct{}
	if err := New(dev, &object); err != nil {
		t.Error("should not error ", err)
	}
}

func Test_New_not_ok(t *testing.T) {
	object := teststruct{}
	if err := New("asdfasdf", &object); err == nil {
		t.Error("should error ", err)
	}
}
