package pail

import (
	"fmt"
	"time"

	"github.com/couchbase/gocb"
)

// Pail is our gocb.Bucket wrapper, providing retry goodness.
type Pail struct {
	*gocb.Bucket
	retries int
}

// New will create a new Pail for you to use
//
// err is here for forwards compatibility
func New(bucket *gocb.Bucket, retries int) (*Pail, error) {
	p := Pail{
		Bucket:  bucket,
		retries: retries,
	}
	return &p, nil
}

// Try will attempt to execute fn up to retries+1 times or until a
// non-connection-related error is seen.
func (p *Pail) Try(retries int, fn RetryFunc) error {
	var err error
	for retries >= 0 {
		retries--
		err = fn(p.Bucket)
		if isConnectErr(err) {
			time.Sleep(20 * time.Millisecond)
		} else if err != nil {
			return err
		} else {
			return nil
		}
	}

	return fmt.Errorf("retries breached (last error: %v", err)
}

// TryGet wraps bucket.Get with a retry func
func (p *Pail) TryGet(key string, valuePtr interface{}) (gocb.Cas, error) {
	var cas gocb.Cas
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { cas, err = b.Get(key, valuePtr); return err }); tryErr != nil {
		return 0, tryErr
	}
	return cas, err
}

// TryTouch wraps bucket.Touch with a retry func
func (p *Pail) TryTouch(key string, cas gocb.Cas, expiry uint32) (gocb.Cas, error) {
	var rcas gocb.Cas
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { rcas, err = b.Touch(key, cas, expiry); return err }); tryErr != nil {
		return rcas, tryErr
	}
	return rcas, err
}

// TryMutateIn wraps bucket.TryMutateIn with our own MutateInBuilder
func (p *Pail) TryMutateIn(key string, cas gocb.Cas, expiry uint32) *MutateInBuilder {
	mib := p.Bucket.MutateIn(key, cas, expiry)
	return &MutateInBuilder{MutateInBuilder: mib, p: p}
}

// TryMutateInEx wraps bucket.TryMutateInEx with our own MutateInBuilder
func (p *Pail) TryMutateInEx(key string, flags gocb.SubdocDocFlag, cas gocb.Cas, expiry uint32) *MutateInBuilder {
	mib := p.Bucket.MutateInEx(key, flags, cas, expiry)
	return &MutateInBuilder{MutateInBuilder: mib, p: p}
}

// TryLookupIn wraps bucket.TryLookupIn with our own LookupInBuilder
func (p *Pail) TryLookupIn(key string) *LookupInBuilder {
	lib := p.Bucket.LookupIn(key)
	return &LookupInBuilder{LookupInBuilder: lib, p: p}
}

// TryLookupInEx wraps bucket.TryLookupInEx with our own LookupInBuilder
func (p *Pail) TryLookupInEx(key string, flags gocb.SubdocDocFlag) *LookupInBuilder {
	lib := p.Bucket.LookupInEx(key, flags)
	return &LookupInBuilder{LookupInBuilder: lib, p: p}
}

// TryUpsert wraps bucket.Upsert with a retry func
func (p *Pail) TryUpsert(key string, value interface{}, expiry uint32) (gocb.Cas, error) {
	var cas gocb.Cas
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { cas, err = b.Upsert(key, value, expiry); return err }); tryErr != nil {
		return 0, tryErr
	}
	return cas, err
}

// TryCounter wraps bucket.Counter with a retry func
func (p *Pail) TryCounter(key string, delta, initial int64, expiry uint32) (uint64, gocb.Cas, error) {
	var cas gocb.Cas
	var count uint64
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { count, cas, err = b.Counter(key, delta, initial, expiry); return err }); tryErr != nil {
		return 0, 0, tryErr
	}
	return count, cas, err
}

// TryInsert wraps bucket.Insert with a retry func
func (p *Pail) TryInsert(key string, value interface{}, expiry uint32) (gocb.Cas, error) {
	var cas gocb.Cas
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { cas, err = b.Insert(key, value, expiry); return err }); tryErr != nil {
		return 0, tryErr
	}
	return cas, err
}

// TryReplace wraps bucket.replace with a retry func
func (p *Pail) TryReplace(key string, value interface{}, cas gocb.Cas, expiry uint32) (gocb.Cas, error) {
	var rcas gocb.Cas
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { rcas, err = b.Replace(key, value, cas, expiry); return err }); tryErr != nil {
		return 0, tryErr
	}
	return rcas, err
}

// TryRemove wraps bucket.Remove with a retry func
func (p *Pail) TryRemove(key string, cas gocb.Cas) (gocb.Cas, error) {
	var rcas gocb.Cas
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { rcas, err = b.Remove(key, cas); return err }); tryErr != nil {
		return 0, tryErr
	}
	return rcas, err
}

// TryExecuteN1qlQuery wraps bucket.ExecuteN1qlQuery with a retry func
func (p *Pail) TryExecuteN1qlQuery(n1ql *gocb.N1qlQuery, params interface{}) (gocb.QueryResults, error) {
	var err error
	var qr gocb.QueryResults
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { qr, err = b.ExecuteN1qlQuery(n1ql, params); return err }); tryErr != nil {
		return nil, tryErr
	}
	return qr, err
}

// TryDo wraps bucket.Do with a retry func
func (p *Pail) TryDo(ops []gocb.BulkOp) error {
	return p.Try(p.retries, func(b *gocb.Bucket) error { return b.Do(ops) })
}

// TryViewQuery - ExecuteViewQuery Try() wrapper.
func (p *Pail) TryViewQuery(vq *gocb.ViewQuery) (gocb.ViewResults, error) {
	var vr gocb.ViewResults
	var err error
	if tryErr := p.Try(p.retries, func(b *gocb.Bucket) error { vr, err = b.ExecuteViewQuery(vq); return err }); tryErr != nil {
		return nil, tryErr
	}
	return vr, err
}

// TryN1qlQueryWithParameters will create and execute a new N1qlQuery type, setting the provided consistency and parameters
// for you
func (p *Pail) TryN1qlQueryWithParameters(query string, consistency gocb.ConsistencyMode, params interface{}) (gocb.QueryResults, error) {
	if params == nil {
		params = []interface{}{}
	}
	nq := gocb.NewN1qlQuery(query).Consistency(consistency)
	return p.TryExecuteN1qlQuery(nq, params)
}

// TryN1qlQueryNotBounded creates and executes a new N1ql query with parameters, setting the NotBounded consistency type
func (p *Pail) TryN1qlQueryNotBounded(query string, params ...interface{}) (gocb.QueryResults, error) {
	if len(params) == 0 {
		return p.TryN1qlQueryWithParameters(query, gocb.NotBounded, nil)
	}
	if _, ok := params[0].(map[string]interface{}); ok {
		return p.TryN1qlQueryWithParameters(query, gocb.NotBounded, params[0])
	}
	return p.TryN1qlQueryWithParameters(query, gocb.NotBounded, params)
}

// TryN1qlQueryRequestPlus creates and executes a new N1ql query with parameters, setting the RequestPlus consistency type
func (p *Pail) TryN1qlQueryRequestPlus(query string, params ...interface{}) (gocb.QueryResults, error) {
	if len(params) == 0 {
		return p.TryN1qlQueryWithParameters(query, gocb.RequestPlus, nil)
	}
	if _, ok := params[0].(map[string]interface{}); ok {
		return p.TryN1qlQueryWithParameters(query, gocb.RequestPlus, params[0])
	}
	return p.TryN1qlQueryWithParameters(query, gocb.RequestPlus, params)
}

// TryN1qlQueryStatementPlus creates and executes a new N1ql query with parameters, setting the StatementPlus consistency type
func (p *Pail) TryN1qlQueryStatementPlus(query string, params ...interface{}) (gocb.QueryResults, error) {
	if len(params) == 0 {
		return p.TryN1qlQueryWithParameters(query, gocb.StatementPlus, nil)
	}
	if _, ok := params[0].(map[string]interface{}); ok {
		return p.TryN1qlQueryWithParameters(query, gocb.StatementPlus, params[0])
	}
	return p.TryN1qlQueryWithParameters(query, gocb.StatementPlus, params)
}
