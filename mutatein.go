package pail

import (
	"github.com/couchbase/gocb"
)

// MutateInBuilder embeds the gocb.MutateInBuilder type, enabling retry functionality
type MutateInBuilder struct {
	*gocb.MutateInBuilder
	p *Pail
}

// TryExecute will attempt the TryMutateIn call repeatedly until success, non-connection error is seen, or retries is
// breached
func (mib *MutateInBuilder) TryExecute() (*gocb.DocumentFragment, error) {
	var df *gocb.DocumentFragment
	var err error
	tryErr := mib.p.Try(mib.p.retries, func(b *gocb.Bucket) error { df, err = mib.MutateInBuilder.Execute(); return err })
	if tryErr != nil {
		return nil, tryErr
	}
	return df, err
}

// ArrayAddUnique wraps the default couchbase ArrayAddUnique with retries
func (mib *MutateInBuilder) ArrayAddUnique(path string, value interface{}, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.ArrayAddUnique(path, value, createParents)
	return mib
}

// ArrayAddUniqueEx wraps the default couchbase ArrayAddUniqueEx with retries
func (mib *MutateInBuilder) ArrayAddUniqueEx(path string, value interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ArrayAddUniqueEx(path, value, flags)
	return mib
}

// ArrayAppend wraps the default couchbase ArrayAppend with retries
func (mib *MutateInBuilder) ArrayAppend(path string, value interface{}, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.ArrayAppend(path, value, createParents)
	return mib
}

// ArrayAppendEx wraps the default couchbase ArrayAppendEx  with retries
func (mib *MutateInBuilder) ArrayAppendEx(path string, value interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ArrayAppendEx(path, value, flags)
	return mib
}

// ArrayAppendMulti wraps the default couchbase ArrayAppendMulti with retries
func (mib *MutateInBuilder) ArrayAppendMulti(path string, values interface{}, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.ArrayAppendMulti(path, values, createParents)
	return mib
}

// ArrayAppendMultiEx wraps the default couchbase ArrayAppendMultiEx with retries
func (mib *MutateInBuilder) ArrayAppendMultiEx(path string, values interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ArrayAppendMultiEx(path, values, flags)
	return mib
}

// ArrayInsert wraps the default couchbase ArrayInsert with retries
func (mib *MutateInBuilder) ArrayInsert(path string, value interface{}) *MutateInBuilder {
	mib.MutateInBuilder.ArrayInsert(path, value)
	return mib
}

// ArrayInsertEx wraps the default couchbase ArrayInsertEx with retries
func (mib *MutateInBuilder) ArrayInsertEx(path string, value interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ArrayInsertEx(path, value, flags)
	return mib
}

// ArrayInsertMulti wraps the default couchbase ArrayInsertMulti with retries
func (mib *MutateInBuilder) ArrayInsertMulti(path string, values interface{}) *MutateInBuilder {
	mib.MutateInBuilder.ArrayInsertMulti(path, values)
	return mib
}

// ArrayInsertMultiEx wraps the default couchbase ArrayInsertMultiEx with retries
func (mib *MutateInBuilder) ArrayInsertMultiEx(path string, values interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ArrayInsertMultiEx(path, values, flags)
	return mib
}

// ArrayPrepend wraps the default couchbase ArrayPrepend with retries
func (mib *MutateInBuilder) ArrayPrepend(path string, value interface{}, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.ArrayPrepend(path, value, createParents)
	return mib
}

// ArrayPrependEx wraps the default couchbase ArrayPrependEx with retries
func (mib *MutateInBuilder) ArrayPrependEx(path string, value interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ArrayPrependEx(path, value, flags)
	return mib
}

// ArrayPrependMulti wraps the default couchbase ArrayPrependMulti with retries
func (mib *MutateInBuilder) ArrayPrependMulti(path string, values interface{}, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.ArrayPrependMulti(path, values, createParents)
	return mib
}

// ArrayPrependMultiEx wraps the default couchbase ArrayPrependMultiEx with retries
func (mib *MutateInBuilder) ArrayPrependMultiEx(path string, values interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ArrayPrependMultiEx(path, values, flags)
	return mib
}

// Counter wraps the default couchbase Counter with retries
func (mib *MutateInBuilder) Counter(path string, delta int64, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.Counter(path, delta, createParents)
	return mib
}

// CounterEx wraps the default couchbase CounterEx with retries
func (mib *MutateInBuilder) CounterEx(path string, delta int64, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.CounterEx(path, delta, flags)
	return mib
}

// Insert wraps the default couchbase Insert with retries
func (mib *MutateInBuilder) Insert(path string, value interface{}, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.Insert(path, value, createParents)
	return mib
}

// InsertEx wraps the default couchbase InsertEx with retries
func (mib *MutateInBuilder) InsertEx(path string, value interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.InsertEx(path, value, flags)
	return mib
}

// Remove wraps the default couchbase Remove with retries
func (mib *MutateInBuilder) Remove(path string) *MutateInBuilder {
	mib.MutateInBuilder.Remove(path)
	return mib
}

// RemoveEx wraps the default couchbase RemoveEx with retries
func (mib *MutateInBuilder) RemoveEx(path string, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.RemoveEx(path, flags)
	return mib
}

// Replace wraps the default Couchbase Replace with retries
func (mib *MutateInBuilder) Replace(path string, value interface{}) *MutateInBuilder {
	mib.MutateInBuilder.Replace(path, value)
	return mib
}

// ReplaceEx wraps the default Couchbase ReplaceEx with retries
func (mib *MutateInBuilder) ReplaceEx(path string, value interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.ReplaceEx(path, value, flags)
	return mib
}

// Upsert wraps the default couchbase Upsert with retries
func (mib *MutateInBuilder) Upsert(path string, value interface{}, createParents bool) *MutateInBuilder {
	mib.MutateInBuilder.Upsert(path, value, createParents)
	return mib
}

// UpsertEx wraps the default couchbase UpsertEx with retries
func (mib *MutateInBuilder) UpsertEx(path string, value interface{}, flags gocb.SubdocFlag) *MutateInBuilder {
	mib.MutateInBuilder.UpsertEx(path, value, flags)
	return mib
}
