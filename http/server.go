package http

import "github.com/labstack/echo/v4"

func New() *Server {
	e := echo.New()
	server := &Server{echo: e}
	return server
}

func (s *Server) GET(path string, handler HandlerFunc, handlers ...HandlerFunc) {
	s.echo.GET(path, func(c echo.Context) error {
		ct := &context{ec: c, server: s}
		return handler(ct)
	}, wrapMiddleware(s, handlers...)...)
}

func (s *Server) POST(path string, handler HandlerFunc, handlers ...HandlerFunc) {
	s.echo.POST(path, func(c echo.Context) error {
		ct := &context{ec: c, server: s}
		return handler(ct)
	}, wrapMiddleware(s, handlers...)...)
}

func (s *Server) PUT(path string, handler HandlerFunc, handlers ...HandlerFunc) {
	s.echo.PUT(path, func(c echo.Context) error {
		ct := &context{ec: c, server: s}
		return handler(ct)
	}, wrapMiddleware(s, handlers...)...)
}

func (s *Server) PATCH(path string, handler HandlerFunc, handlers ...HandlerFunc) {
	s.echo.PATCH(path, func(c echo.Context) error {
		ct := &context{ec: c, server: s}
		return handler(ct)
	}, wrapMiddleware(s, handlers...)...)
}

func (s *Server) DELETE(path string, handler HandlerFunc, handlers ...HandlerFunc) {
	s.echo.DELETE(path, func(c echo.Context) error {
		ct := &context{ec: c, server: s}
		return handler(ct)
	}, wrapMiddleware(s, handlers...)...)
}

func (s *Server) Use(handler HandlerFunc) {
	s.echo.Use(wrapMiddleware(s, handler)[0])
}

func wrapMiddleware(server *Server, handlers ...HandlerFunc) []echo.MiddlewareFunc {
	middleware := make([]echo.MiddlewareFunc, 0)

	filter := func(before HandlerFunc) echo.MiddlewareFunc {
		return func(current echo.HandlerFunc) echo.HandlerFunc {
			return func(c echo.Context) error {
				ct := &context{ec: c, server: server}
				if err := before(ct); err != nil {
					return err
				}
				return current(c)
			}
		}
	}

	for _, before := range handlers {
		middleware = append(middleware, filter(before))
	}
	return middleware
}
