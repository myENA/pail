package pail

import (
	"github.com/couchbase/gocb"
)

// this is a list of errors deemed to probably be related to a connection issue.
var cbConnectionErrors = map[error]struct{}{
	gocb.ErrNoOpenBuckets:      {},
	gocb.ErrDispatchFail:       {},
	gocb.ErrShutdown:           {},
	gocb.ErrOverload:           {},
	gocb.ErrNetwork:            {},
	gocb.ErrTimeout:            {},
	gocb.ErrCliInternalError:   {},
	gocb.ErrStreamClosed:       {},
	gocb.ErrStreamStateChanged: {},
	gocb.ErrStreamDisconnected: {},
	gocb.ErrStreamTooSlow:      {},
}

// Given an error, determine if it's a connection related error and return
// true if so.
func isConnectErr(err error) bool {
	if err == nil {
		return false
	}
	_, ok := cbConnectionErrors[err]
	return ok
}
