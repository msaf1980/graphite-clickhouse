package utils

import (
	"strings"
	"time"
)

type StringSlice []string

func (u *StringSlice) Set(value string) error {
	*u = append(*u, value)
	return nil
}

func (u *StringSlice) String() string {
	return "[ " + strings.Join(*u, ", ") + " ]"
}

func (u *StringSlice) Type() string {
	return "[]string"
}

// TimestampTruncate truncate timestamp with duration
func TimestampTruncate(ts int64, duration time.Duration) int64 {
	tm := time.Unix(ts, 0).UTC()
	return tm.Truncate(duration).UTC().Unix()
}
