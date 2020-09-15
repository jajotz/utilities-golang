package webclient

import (
	"io"
	"net/http"
	"time"
)

type (
	WebClientFactory interface {
		Create(timeout time.Duration) Client
	}

	Client interface {
		Get(url string, headers http.Header) (*http.Response, error)
		Post(url string, body io.Reader, headers http.Header) (*http.Response, error)
		Put(url string, body io.Reader, headers http.Header) (*http.Response, error)
		Patch(url string, body io.Reader, headers http.Header) (*http.Response, error)
		Delete(url string, headers http.Header) (*http.Response, error)
		Do(req *http.Request) (*http.Response, error)
	}
)
