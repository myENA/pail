package pail

import (
	"time"

	"github.com/couchbase/gocb/v2"
	cbsearch "github.com/couchbase/gocb/v2/search"
)

type Cluster struct {
	*gocb.Cluster
	retries uint32
	delay   time.Duration
}

func NewCluster(cluster *gocb.Cluster, retries int, delay time.Duration) *Cluster {
	c := new(Cluster)
	c.Cluster = cluster
	c.retries = uint32(retries)
	c.delay = delay
	return c
}

func (c *Cluster) Bucket(bucketName string) *Pail {
	return NewPail(c.Cluster.Bucket(bucketName), int(c.retries), c.delay)
}

func (c *Cluster) QueryOptions(in *gocb.QueryOptions, fn ClusterRetryFunc) (ClusterRetryContext, *gocb.QueryOptions) {
	out := new(gocb.QueryOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultClusterRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Cluster) SearchOptions(in *gocb.SearchOptions, fn ClusterRetryFunc) (ClusterRetryContext, *gocb.SearchOptions) {
	out := new(gocb.SearchOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultClusterRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Cluster) Try(ctx ClusterRetryContext) error {
	return ctx.Try(c.Cluster)
}

func (c *Cluster) Query(statement string, opts *gocb.QueryOptions) (*gocb.QueryResult, error) {
	var (
		res *gocb.QueryResult
		ctx ClusterRetryContext
		err error
	)
	ctx, opts = c.QueryOptions(opts, func(cluster *gocb.Cluster) error { res, err = cluster.Query(statement, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Cluster) SearchQuery(indexName string, query cbsearch.Query, opts *gocb.SearchOptions) (*gocb.SearchResult, error) {
	var (
		res *gocb.SearchResult
		ctx ClusterRetryContext
		err error
	)
	ctx, opts = c.SearchOptions(opts, func(c *gocb.Cluster) error { res, err = c.SearchQuery(indexName, query, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

// Pail is our gocb.Bucket wrapper, providing retry goodness.
type Pail struct {
	*gocb.Bucket
	retries uint32
	delay   time.Duration
}

func NewPail(bucket *gocb.Bucket, retries int, delay time.Duration) *Pail {
	p := new(Pail)
	p.Bucket = bucket
	p.retries = uint32(retries)
	p.delay = delay
	return p
}

// Try will attempt to execute retryFunc up to retries+1 times or until a
// non-connection-related error is seen.
func (p *Pail) Try(ctx BucketRetryContext) error {
	return ctx.Try(p.Bucket)
}

func (p *Pail) GetOptions(in *gocb.GetOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.GetOptions) {
	out := new(gocb.GetOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) TouchOptions(in *gocb.TouchOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.TouchOptions) {
	out := new(gocb.TouchOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) UpsertOptions(in *gocb.UpsertOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.UpsertOptions) {
	out := new(gocb.UpsertOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) InsertOptions(in *gocb.InsertOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.InsertOptions) {
	out := new(gocb.InsertOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) ReplaceOptions(in *gocb.ReplaceOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.ReplaceOptions) {
	out := new(gocb.ReplaceOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) RemoveOptions(in *gocb.RemoveOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.RemoveOptions) {
	out := new(gocb.RemoveOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) IncrementOptions(in *gocb.IncrementOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.IncrementOptions) {
	out := new(gocb.IncrementOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) DecrementOptions(in *gocb.DecrementOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.DecrementOptions) {
	out := new(gocb.DecrementOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) AppendOptions(in *gocb.AppendOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.AppendOptions) {
	out := new(gocb.AppendOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) BulkOpOptions(in *gocb.BulkOpOptions, fn BucketRetryFunc) (BucketRetryContext, *gocb.BulkOpOptions) {
	out := new(gocb.BulkOpOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewDefaultBucketRetryContext(p.retries, p.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (p *Pail) TryDo(ops []gocb.BulkOp, opts *gocb.BulkOpOptions) error {
	var (
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.BulkOpOptions(opts, func(b *gocb.Bucket) error { err = b.DefaultCollection().Do(ops, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return tryErr
	}
	return err
}

func (p *Pail) TryGet(id string, opts *gocb.GetOptions) (*gocb.GetResult, error) {
	var (
		res *gocb.GetResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.GetOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Get(id, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (p *Pail) TryGetModeled(id string, opts *gocb.GetOptions, ptr interface{}) (*gocb.GetResult, error) {
	var (
		res *gocb.GetResult
		err error
	)
	if res, err = p.TryGet(id, opts); err != nil {
		return nil, err
	}
	return res, res.Content(ptr)
}

func (p *Pail) TryTouch(id string, expiry time.Duration, opts *gocb.TouchOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.TouchOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Touch(id, expiry, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (p *Pail) TryUpsert(id string, value interface{}, opts *gocb.UpsertOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.UpsertOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Upsert(id, value, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (p *Pail) TryInsert(id string, value interface{}, opts *gocb.InsertOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.InsertOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Insert(id, value, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (p *Pail) TryReplace(id string, value interface{}, opts *gocb.ReplaceOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.ReplaceOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Replace(id, value, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (p *Pail) TryRemove(id string, opts *gocb.RemoveOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.RemoveOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Remove(id, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (p *Pail) TryMutateIn(id string, cas gocb.Cas, expiry uint32) *MutateInBuilder {
	mib := p.Bucket.DefaultCollection().MutateIn(id, cas, expiry)
	return &MutateInBuilder{MutateInBuilder: mib, p: p}
}

func (p *Pail) TryMutateInEx(id string, flags gocb.SubdocDocFlag, cas gocb.Cas, expiry uint32) *MutateInBuilder {
	mib := p.Bucket.MutateInEx(id, flags, cas, expiry)
	return &MutateInBuilder{MutateInBuilder: mib, p: p}
}

func (p *Pail) TryLookupIn(id string) *LookupInBuilder {
	lib := p.Bucket.LookupIn(id)
	return &LookupInBuilder{LookupInBuilder: lib, p: p}
}

func (p *Pail) TryLookupInEx(id string, flags gocb.SubdocDocFlag) *LookupInBuilder {
	lib := p.Bucket.LookupInEx(id, flags)
	return &LookupInBuilder{LookupInBuilder: lib, p: p}
}

func (p *Pail) TryIncrement(id string, opts *gocb.IncrementOptions) (*gocb.CounterResult, error) {
	var (
		res *gocb.CounterResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.IncrementOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Binary().Increment(id, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (p *Pail) TryDecrement(id string, opts *gocb.DecrementOptions) (*gocb.CounterResult, error) {
	var (
		res *gocb.CounterResult
		ctx BucketRetryContext
		err error
	)
	ctx, opts = p.DecrementOptions(opts, func(b *gocb.Bucket) error { res, err = b.DefaultCollection().Binary().Decrement(id, opts); return err })
	if tryErr := p.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}
