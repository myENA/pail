package pail

import (
	"github.com/couchbase/gocb"
)

// RetryFunc - this is passed to Try
type RetryFunc func(*gocb.Bucket) error
