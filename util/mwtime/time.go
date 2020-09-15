package mwtime

import "time"

type (
	Time struct {
		time.Time
	}
)

func (t Time) UnixMilli() int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
