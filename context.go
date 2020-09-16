package pail

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb/v2"
)

type (
	CollectionRetryFunc func(*gocb.Collection) error
	ClusterRetryFunc    func(*gocb.Cluster) error
)

// this is a list of errors deemed to probably be related to a connection issue.
var cbConnectionErrors = []error{
	gocb.ErrOverload,
	gocb.ErrTimeout,
}

// Given an error, determine if it's a connection related error and return
// true if so.
func isConnectErr(err error) bool {
	if err == nil {
		return false
	}
	for _, connErr := range cbConnectionErrors {
		if errors.Is(err, connErr) {
			return true
		}
	}
	return false
}

type ConnectionErrorRetryAction time.Duration

func (a ConnectionErrorRetryAction) Duration() time.Duration { return time.Duration(a) }

type baseRetryContext struct {
	tries        uint32
	retries      uint32
	action       ConnectionErrorRetryAction
	baseStrategy gocb.RetryStrategy
}

func newBaseRetryContext(retries uint32, delay time.Duration, baseStrategy gocb.RetryStrategy) baseRetryContext {
	return baseRetryContext{
		retries:      retries,
		action:       ConnectionErrorRetryAction(delay),
		baseStrategy: baseStrategy,
	}
}

func (bc baseRetryContext) RetryAfter(req gocb.RetryRequest, reason gocb.RetryReason) gocb.RetryAction {
	// if base strategy provided, defer to it
	if bc.baseStrategy != nil {
		// increment counter only if this is not an "always retry" reason
		if !reason.AlwaysRetry() {
			atomic.AddUint32(&bc.tries, 1)
		}
		return bc.baseStrategy.RetryAfter(req, reason)
	}
	// if the source reason stems from an "always retry" error i.e., incorrect node queried for a particular vbucket,
	// always attempt again
	if reason.AlwaysRetry() {
		return bc.action
	}
	// test for breach of retry limit
	if t := atomic.AddUint32(&bc.tries, 1); t > bc.retries {
		return ConnectionErrorRetryAction(0)
	}
	// try again, plz.
	return bc.action
}

type CollectionRetryContext interface {
	gocb.RetryStrategy
	Try(*gocb.Collection) error
}

type SimpleCollectionRetryContext struct {
	baseRetryContext
	retryFunc CollectionRetryFunc
}

func NewSimpleCollectionRetryContext(retries uint32, delay time.Duration, baseStrategy gocb.RetryStrategy, fn CollectionRetryFunc) SimpleCollectionRetryContext {
	rc := SimpleCollectionRetryContext{
		baseRetryContext: newBaseRetryContext(retries, delay, baseStrategy),
		retryFunc:        fn,
	}
	return rc
}

func (rc SimpleCollectionRetryContext) Try(b *gocb.Collection) error {
	var (
		t   uint32
		err error
	)
	for t = atomic.AddUint32(&rc.retries, 1); t <= rc.retries; {
		if err = rc.retryFunc(b); err == nil {
			return nil
		} else if isConnectErr(err) {
			time.Sleep(time.Duration(rc.action))
		} else {
			return err
		}
	}
	return fmt.Errorf("retries breached (last error: %w)", err)
}

type ClusterRetryContext interface {
	gocb.RetryStrategy
	Try(*gocb.Cluster) error
}

type DefaultClusterRetryContext struct {
	baseRetryContext
	retryFunc ClusterRetryFunc
}

func NewDefaultClusterRetryContext(retries uint32, delay time.Duration, baseStrategy gocb.RetryStrategy, fn ClusterRetryFunc) DefaultClusterRetryContext {
	rc := DefaultClusterRetryContext{
		baseRetryContext: newBaseRetryContext(retries, delay, baseStrategy),
		retryFunc:        fn,
	}
	return rc
}

func (rc DefaultClusterRetryContext) Try(c *gocb.Cluster) error {
	var (
		t   uint32
		err error
	)
	for t = atomic.AddUint32(&rc.retries, 1); t <= rc.retries; {
		if err = rc.retryFunc(c); err == nil {
			return nil
		} else if isConnectErr(err) {
			time.Sleep(time.Duration(rc.action))
		} else {
			return err
		}
	}
	return fmt.Errorf("retries breached (last error: %w)", err)
}
