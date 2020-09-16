package pail

import (
	"time"

	"github.com/couchbase/gocb/v2"
	cbsearch "github.com/couchbase/gocb/v2/search"
)

const (
	defaultThingName = "_default"
)

type commonRetryable struct {
	retries uint32
	delay   time.Duration
}

func Connect(connStr string, opts gocb.ClusterOptions, retries int, delay time.Duration) (*Cluster, error) {
	cluster, err := gocb.Connect(connStr, opts)
	if err != nil {
		return nil, err
	}
	return NewCluster(cluster, retries, delay), nil
}

type Cluster struct {
	*gocb.Cluster
	commonRetryable
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

func (c *Cluster) QueryIndexes() *QueryIndexManager {
	return NewQueryIndexManager(c.Cluster.QueryIndexes(), int(c.retries), c.delay)
}

func (c *Cluster) QueryOptions(in *gocb.QueryOptions, fn ClusterRetryFunc) (ClusterRetryContext, *gocb.QueryOptions) {
	out := new(gocb.QueryOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleClusterRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Cluster) SearchOptions(in *gocb.SearchOptions, fn ClusterRetryFunc) (ClusterRetryContext, *gocb.SearchOptions) {
	out := new(gocb.SearchOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleClusterRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
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

type QueryIndexManager struct {
	*gocb.QueryIndexManager
	commonRetryable
}

func NewQueryIndexManager(queryIndexManager *gocb.QueryIndexManager, retries int, delay time.Duration) *QueryIndexManager {
	qm := new(QueryIndexManager)
	qm.QueryIndexManager = queryIndexManager
	qm.retries = uint32(retries)
	qm.delay = delay
	return qm
}

func (qm *QueryIndexManager) Try(ctx QueryIndexManagerRetryContext) error {
	return ctx.Try(qm.QueryIndexManager)
}

func (qm *QueryIndexManager) CreateQueryIndexOptions(in *gocb.CreateQueryIndexOptions, fn QueryIndexManagerRetryFunc) (QueryIndexManagerRetryContext, *gocb.CreateQueryIndexOptions) {
	out := new(gocb.CreateQueryIndexOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleQueryIndexManagerRetryContext(qm.retries, qm.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (qm *QueryIndexManager) CreatePrimaryQueryIndexOptions(in *gocb.CreatePrimaryQueryIndexOptions, fn QueryIndexManagerRetryFunc) (QueryIndexManagerRetryContext, *gocb.CreatePrimaryQueryIndexOptions) {
	out := new(gocb.CreatePrimaryQueryIndexOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleQueryIndexManagerRetryContext(qm.retries, qm.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (qm *QueryIndexManager) DropQueryIndexOptions(in *gocb.DropQueryIndexOptions, fn QueryIndexManagerRetryFunc) (QueryIndexManagerRetryContext, *gocb.DropQueryIndexOptions) {
	out := new(gocb.DropQueryIndexOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleQueryIndexManagerRetryContext(qm.retries, qm.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (qm *QueryIndexManager) DropPrimaryQueryIndexOptions(in *gocb.DropPrimaryQueryIndexOptions, fn QueryIndexManagerRetryFunc) (QueryIndexManagerRetryContext, *gocb.DropPrimaryQueryIndexOptions) {
	out := new(gocb.DropPrimaryQueryIndexOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleQueryIndexManagerRetryContext(qm.retries, qm.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (qm *QueryIndexManager) GetAllQueryIndexesOptions(in *gocb.GetAllQueryIndexesOptions, fn QueryIndexManagerRetryFunc) (QueryIndexManagerRetryContext, *gocb.GetAllQueryIndexesOptions) {
	out := new(gocb.GetAllQueryIndexesOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleQueryIndexManagerRetryContext(qm.retries, qm.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (qm *QueryIndexManager) BuildDeferredQueryIndexOptions(in *gocb.BuildDeferredQueryIndexOptions, fn QueryIndexManagerRetryFunc) (QueryIndexManagerRetryContext, *gocb.BuildDeferredQueryIndexOptions) {
	out := new(gocb.BuildDeferredQueryIndexOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleQueryIndexManagerRetryContext(qm.retries, qm.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (qm *QueryIndexManager) WatchQueryIndexOptions(in *gocb.WatchQueryIndexOptions, fn QueryIndexManagerRetryFunc) (QueryIndexManagerRetryContext, *gocb.WatchQueryIndexOptions) {
	out := new(gocb.WatchQueryIndexOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleQueryIndexManagerRetryContext(qm.retries, qm.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (qm *QueryIndexManager) CreateIndex(bucketName, indexName string, fields []string, opts *gocb.CreateQueryIndexOptions) error {
	var (
		ctx QueryIndexManagerRetryContext
		err error
	)
	ctx, opts = qm.CreateQueryIndexOptions(opts, func(qm *gocb.QueryIndexManager) error { return qm.CreateIndex(bucketName, indexName, fields, opts) })
	if tryErr := qm.Try(ctx); tryErr != nil {
		return tryErr
	}
	return err
}

func (qm *QueryIndexManager) CreatePrimaryIndex(bucketName string, opts *gocb.CreatePrimaryQueryIndexOptions) error {
	var (
		ctx QueryIndexManagerRetryContext
		err error
	)
	ctx, opts = qm.CreatePrimaryQueryIndexOptions(opts, func(qm *gocb.QueryIndexManager) error { return qm.CreatePrimaryIndex(bucketName, opts) })
	if tryErr := qm.Try(ctx); tryErr != nil {
		return tryErr
	}
	return err
}

func (qm *QueryIndexManager) DropIndex(bucketName, indexName string, opts *gocb.DropQueryIndexOptions) error {
	var (
		ctx QueryIndexManagerRetryContext
		err error
	)
	ctx, opts = qm.DropQueryIndexOptions(opts, func(qm *gocb.QueryIndexManager) error { return qm.DropIndex(bucketName, indexName, opts) })
	if tryErr := qm.Try(ctx); tryErr != nil {
		return tryErr
	}
	return err
}

func (qm *QueryIndexManager) DropPrimaryIndex(bucketName string, opts *gocb.DropPrimaryQueryIndexOptions) error {
	var (
		ctx QueryIndexManagerRetryContext
		err error
	)
	ctx, opts = qm.DropPrimaryQueryIndexOptions(opts, func(qm *gocb.QueryIndexManager) error { return qm.DropPrimaryIndex(bucketName, opts) })
	if tryErr := qm.Try(ctx); tryErr != nil {
		return tryErr
	}
	return err
}

func (qm *QueryIndexManager) GetAllIndexes(bucketName string, opts *gocb.GetAllQueryIndexesOptions) ([]gocb.QueryIndex, error) {
	var (
		res []gocb.QueryIndex
		ctx QueryIndexManagerRetryContext
		err error
	)
	ctx, opts = qm.GetAllQueryIndexesOptions(opts, func(qm *gocb.QueryIndexManager) error { res, err = qm.GetAllIndexes(bucketName, opts); return err })
	if tryErr := qm.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (qm *QueryIndexManager) BuildDeferredIndexes(bucketName string, opts *gocb.BuildDeferredQueryIndexOptions) ([]string, error) {
	var (
		res []string
		ctx QueryIndexManagerRetryContext
		err error
	)
	ctx, opts = qm.BuildDeferredQueryIndexOptions(opts, func(qm *gocb.QueryIndexManager) error {
		res, err = qm.BuildDeferredIndexes(bucketName, opts)
		return err
	})
	if tryErr := qm.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

// Pail is our gocb.Bucket wrapper, providing retry goodness.
type Pail struct {
	*gocb.Bucket
	commonRetryable
}

func NewPail(bucket *gocb.Bucket, retries int, delay time.Duration) *Pail {
	p := new(Pail)
	p.Bucket = bucket
	p.retries = uint32(retries)
	p.delay = delay
	return p
}

func (p *Pail) Scope(scopeName string) *Scope {
	scope := new(Scope)
	scope.Scope = p.Bucket.Scope(scopeName)
	return scope
}

func (p *Pail) DefaultScope() *Scope {
	return p.Scope(defaultThingName)
}

func (p *Pail) Collection(collectionName string) *Collection {
	return p.DefaultScope().Collection(collectionName)
}

func (p *Pail) DefaultCollection() *Collection {
	return p.Collection(defaultThingName)
}

func (p *Pail) ScopeCollection(scopeName, collectionName string) *Collection {
	return p.Scope(scopeName).Collection(collectionName)
}

type Scope struct {
	*gocb.Scope
	commonRetryable
}

func (s *Scope) Collection(collectionName string) *Collection {
	c := new(Collection)
	c.Collection = s.Scope.Collection(collectionName)
	return c
}

func (s *Scope) DefaultCollection() *Collection {
	return s.Collection(defaultThingName)
}

type Collection struct {
	*gocb.Collection
	commonRetryable
}

// Try will attempt to execute retryFunc up to retries+1 times or until a
// non-connection-related error is seen.
func (c *Collection) Try(ctx CollectionRetryContext) error {
	return ctx.Try(c.Collection)
}

func (c *Collection) GetOptions(in *gocb.GetOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.GetOptions) {
	out := new(gocb.GetOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) TouchOptions(in *gocb.TouchOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.TouchOptions) {
	out := new(gocb.TouchOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) UpsertOptions(in *gocb.UpsertOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.UpsertOptions) {
	out := new(gocb.UpsertOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) InsertOptions(in *gocb.InsertOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.InsertOptions) {
	out := new(gocb.InsertOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) ReplaceOptions(in *gocb.ReplaceOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.ReplaceOptions) {
	out := new(gocb.ReplaceOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) RemoveOptions(in *gocb.RemoveOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.RemoveOptions) {
	out := new(gocb.RemoveOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) IncrementOptions(in *gocb.IncrementOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.IncrementOptions) {
	out := new(gocb.IncrementOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) DecrementOptions(in *gocb.DecrementOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.DecrementOptions) {
	out := new(gocb.DecrementOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) AppendOptions(in *gocb.AppendOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.AppendOptions) {
	out := new(gocb.AppendOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}
func (c *Collection) PrependOptions(in *gocb.PrependOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.PrependOptions) {
	out := new(gocb.PrependOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) BulkOpOptions(in *gocb.BulkOpOptions, fn CollectionRetryFunc) (CollectionRetryContext, *gocb.BulkOpOptions) {
	out := new(gocb.BulkOpOptions)
	if in != nil {
		*out = *in
	}
	ctx := NewSimpleCollectionRetryContext(c.retries, c.delay, out.RetryStrategy, fn)
	out.RetryStrategy = ctx
	return ctx, out
}

func (c *Collection) TryDo(ops []gocb.BulkOp, opts *gocb.BulkOpOptions) error {
	var (
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.BulkOpOptions(opts, func(c *gocb.Collection) error { err = c.Do(ops, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return tryErr
	}
	return err
}

func (c *Collection) TryGet(id string, opts *gocb.GetOptions) (*gocb.GetResult, error) {
	var (
		res *gocb.GetResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.GetOptions(opts, func(c *gocb.Collection) error { res, err = c.Get(id, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryGetContent(id string, ptr interface{}, opts *gocb.GetOptions) (*gocb.GetResult, error) {
	var (
		res *gocb.GetResult
		err error
	)
	if res, err = c.TryGet(id, opts); err != nil {
		return nil, err
	}
	return res, res.Content(ptr)
}

func (c *Collection) TryTouch(id string, expiry time.Duration, opts *gocb.TouchOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.TouchOptions(opts, func(c *gocb.Collection) error { res, err = c.Touch(id, expiry, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryUpsert(id string, value interface{}, opts *gocb.UpsertOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.UpsertOptions(opts, func(c *gocb.Collection) error { res, err = c.Upsert(id, value, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryInsert(id string, value interface{}, opts *gocb.InsertOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.InsertOptions(opts, func(c *gocb.Collection) error { res, err = c.Insert(id, value, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryReplace(id string, value interface{}, opts *gocb.ReplaceOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.ReplaceOptions(opts, func(c *gocb.Collection) error { res, err = c.Replace(id, value, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryRemove(id string, opts *gocb.RemoveOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.RemoveOptions(opts, func(c *gocb.Collection) error { res, err = c.Remove(id, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryIncrement(id string, opts *gocb.IncrementOptions) (*gocb.CounterResult, error) {
	var (
		res *gocb.CounterResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.IncrementOptions(opts, func(c *gocb.Collection) error { res, err = c.Binary().Increment(id, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryDecrement(id string, opts *gocb.DecrementOptions) (*gocb.CounterResult, error) {
	var (
		res *gocb.CounterResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.DecrementOptions(opts, func(c *gocb.Collection) error { res, err = c.Binary().Decrement(id, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryAppend(id string, value []byte, opts *gocb.AppendOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.AppendOptions(opts, func(c *gocb.Collection) error { res, err = c.Binary().Append(id, value, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}

func (c *Collection) TryPrepend(id string, value []byte, opts *gocb.PrependOptions) (*gocb.MutationResult, error) {
	var (
		res *gocb.MutationResult
		ctx CollectionRetryContext
		err error
	)
	ctx, opts = c.PrependOptions(opts, func(c *gocb.Collection) error { res, err = c.Binary().Prepend(id, value, opts); return err })
	if tryErr := c.Try(ctx); tryErr != nil {
		return nil, tryErr
	}
	return res, err
}
