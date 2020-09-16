package pail

import (
	"github.com/couchbase/gocb"
)

// LookupInBuilder embeds the gocb.LookupInBuilder type, enabling retry functionality
type LookupInBuilder struct {
	*gocb.LookupInBuilder
	p *Pail
}

// TryExecute will repeatedly execute the TryLookupIn call until success, non-connection error is seen, or retries is
// breached
func (lib *LookupInBuilder) TryExecute() (*gocb.DocumentFragment, error) {
	var df *gocb.DocumentFragment
	var err error
	tryErr := lib.p.TryBucketOp(lib.p.retries, func(b *gocb.Bucket) error { df, err = lib.LookupInBuilder.Execute(); return err })
	if tryErr != nil {
		return nil, tryErr
	}
	return df, err
}

// Exists wraps the default couchbase Exists with retries
func (lib *LookupInBuilder) Exists(path string) *LookupInBuilder {
	lib.LookupInBuilder.Exists(path)
	return lib
}

// ExistsEx wraps the default couchbase ExistsEx with retries
func (lib *LookupInBuilder) ExistsEx(path string, flags gocb.SubdocFlag) *LookupInBuilder {
	lib.LookupInBuilder.ExistsEx(path, flags)
	return lib
}

// Get wraps the default couchbase Get with retries
func (lib *LookupInBuilder) Get(path string) *LookupInBuilder {
	lib.LookupInBuilder.Get(path)
	return lib
}

// GetCount wraps the default couchbase GetCount with retries
func (lib *LookupInBuilder) GetCount(path string) *LookupInBuilder {
	lib.LookupInBuilder.GetCount(path)
	return lib
}

// GetCountEx wraps the default couchbase GetCountEx with retries
func (lib *LookupInBuilder) GetCountEx(path string, flags gocb.SubdocFlag) *LookupInBuilder {
	lib.LookupInBuilder.GetCountEx(path, flags)
	return lib
}

// GetEx wraps the default couchbase GetEx with retries
func (lib *LookupInBuilder) GetEx(path string, flags gocb.SubdocFlag) *LookupInBuilder {
	lib.LookupInBuilder.GetEx(path, flags)
	return lib
}
