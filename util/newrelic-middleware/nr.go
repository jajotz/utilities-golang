package newrelic_middleware

import (
	"fmt"
	"net/http"
	"time"
	shared_dto "utilities-golang/shared/dto"

	"github.com/labstack/echo/v4"
	newrelic "github.com/newrelic/go-agent"
	"github.com/newrelic/go-agent/_integrations/nrpkgerrors"
	"github.com/pkg/errors"
)

func NewRelicMiddleware(app newrelic.Application) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			rw := c.Response().Writer

			txName := fmt.Sprintf("%s [%s]", c.Path(), c.Request().Method)
			tx := app.StartTransaction(txName, c.Response().Writer, c.Request())

			c.Response().Writer = tx
			c.SetRequest(c.Request().WithContext(newrelic.NewContext(c.Request().Context(), tx)))

			appContext := shared_dto.AppContext{
				Context:             c,
				MandatoryRequestDto: shared_dto.MandatoryRequestDto{},
			}

			defer deferNewRelic(c, tx)

			err := next(appContext)

			if err != nil {
				_ = tx.NoticeError(nrpkgerrors.Wrap(err))
				tx.SetWebResponse(nil)

				c.Response().Writer = rw

				if httperr, ok := err.(*echo.HTTPError); ok {
					tx.WriteHeader(httperr.Code)
				} else {
					tx.WriteHeader(http.StatusInternalServerError)
				}

				dto := &shared_dto.BaseResponseDto{
					Code:       "",
					Message:    "",
					Data:       nil,
					ServerTime: time.Now().Unix(),
					Errors:     []string{},
				}

				switch wrapped := errors.Cause(err).(type) {
				case *shared_dto.ErrorCode:
					dto.Message = wrapped.Message
					dto.Code = wrapped.Code
					dto.Errors = append(dto.Errors, wrapped.FrontEndErrorMessage)
					c.Logger().Errorf("%+v\n", wrapped.WrappedError)
				default:
					dto.Code = "INTERNAL_SERVER_ERROR"
					dto.Message = "Internal Server Error"
					dto.Errors = append(dto.Errors, err.Error())
					c.Logger().Errorf("%+v\n", err)
				}

				return c.JSON(200, dto)
			}
			return err
		}
	}
}

func deferNewRelic(c echo.Context, tx newrelic.Transaction) {
	if r := recover(); r != nil {
		err, ok := r.(error)

		if !ok {
			err = fmt.Errorf("PANIC: %v", r)
		}

		_ = tx.NoticeError(nrpkgerrors.Wrap(err))
		c.Logger().Errorf("Recover from panic %s", err.Error())
	}
	_ = tx.End()
}
