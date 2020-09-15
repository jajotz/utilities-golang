package http

import (
	"mime/multipart"
	"net/http"
	"net/url"

	"github.com/jajotz/utilities-golang/logs"

	"github.com/labstack/echo/v4"
)

type (
	// RequestContext abstract implementation of echo.Context
	RequestContext interface {
		String(int, string) error
		JSON(int, interface{}) error
		Redirect(int, string) error
		Error(error)
		Logger() logs.Logger

		Param(string) string
		ParamNames() []string
		ParamValues() []string

		QueryParam(string) string
		QueryParams() url.Values
		QueryString() string

		FormValue(string) string
		FormParams() (url.Values, error)
		FormFile(string) (*multipart.FileHeader, error)
		MultipartForm() (*multipart.Form, error)
		Attachment(string, string) error

		Cookie(string) (*http.Cookie, error)
		SetCookie(*http.Cookie)
		Cookies() []*http.Cookie

		Bind(interface{}) error
		Validate(interface{}) error
	}

	// HandlerFunc defines a function to serve HTTP requests.
	HandlerFunc func(RequestContext) error

	// Server ...
	// abstract implementation of echo.Echo
	Server struct {
		echo   *echo.Echo
		Logger logs.Logger
	}

	context struct {
		ec     echo.Context
		server *Server
	}
)

func (c *context) String(code int, body string) error {
	return c.ec.String(code, body)
}

func (c *context) JSON(code int, body interface{}) error {
	return c.ec.JSON(code, body)
}

func (c *context) Redirect(code int, url string) error {
	return c.ec.JSON(code, url)
}

func (c *context) Error(err error) {
	c.ec.Error(err)
}

func (c *context) Logger() logs.Logger {
	return c.server.Logger
}

func (c *context) Param(name string) string {
	return c.ec.Param(name)
}

func (c *context) ParamNames() []string {
	return c.ec.ParamNames()
}

func (c *context) ParamValues() []string {
	return c.ec.ParamValues()
}

func (c *context) QueryParam(name string) string {
	return c.ec.QueryParam(name)
}

func (c *context) QueryParams() url.Values {
	return c.ec.QueryParams()
}

func (c *context) QueryString() string {
	return c.ec.QueryString()
}

func (c *context) FormValue(key string) string {
	return c.ec.FormValue(key)
}

func (c *context) FormParams() (url.Values, error) {
	return c.ec.FormParams()
}

func (c *context) FormFile(key string) (*multipart.FileHeader, error) {
	return c.ec.FormFile(key)
}

func (c *context) MultipartForm() (*multipart.Form, error) {
	return c.ec.MultipartForm()
}

func (c *context) Attachment(file, name string) error {
	return c.ec.Attachment(file, name)
}

func (c *context) Cookie(name string) (*http.Cookie, error) {
	return c.ec.Cookie(name)
}

func (c *context) SetCookie(cookie *http.Cookie) {
	c.ec.SetCookie(cookie)
}

func (c *context) Cookies() []*http.Cookie {
	return c.ec.Cookies()
}

func (c *context) Bind(object interface{}) error {
	return c.ec.Bind(object)
}

func (c *context) Validate(object interface{}) error {
	return c.ec.Validate(object)
}

func (s *Server) Start(address string) error {
	return s.echo.Start(address)
}
