package store

import (
	"github.com/google/btree"
	"reflect"
	"sort"
)

// This file contains all the code related to an individual page in scotty

var (
	gTsAndValueSize = tsAndValueSize()
)

func tsAndValueSize() int {
	var p pageType
	return int(reflect.TypeOf(p).Elem().Size())
}

// basicPageType is the interface that all page data must implement
type basicPageType interface {
	Clear()
	IsFull() bool
	FindGreaterOrEqual(ts float64) int
	FindGreater(ts float64) int
	Len() int
	StoreIndexToRecord(idx int, record *Record)
}

// We use this same struct for storing timestamp value pairs and for
// storing just timestamps so that we can use a given page for either data
// structure without needing to use the "unsafe" package.
type tsValueType struct {
	TimeStamp float64
	Value     interface{} // Wasted when we store only timestamps.
}

// a single page of timestamps
type tsPageType []tsValueType

func (p tsPageType) IsFull() bool {
	return len(p) == cap(p)
}

func (p *tsPageType) Clear() {
	*p = (*p)[:0]
}

func (p tsPageType) Len() int {
	return len(p)
}

func (p *tsPageType) Add(ts float64) {
	length := len(*p)
	*p = (*p)[0 : length+1]
	(*p)[length].TimeStamp = ts
}

func (p tsPageType) StoreIndexToRecord(idx int, record *Record) {
	record.TimeStamp = p[idx].TimeStamp
}

func (p tsPageType) FindGreaterOrEqual(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp >= ts })
}

func (p tsPageType) FindGreater(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp > ts })
}

// single page of timestamps with values
type pageType []tsValueType

func (p pageType) Len() int {
	return len(p)
}

func (p *pageType) Add(val tsValueType) {
	length := len(*p)
	*p = (*p)[0 : length+1]
	(*p)[length] = val
}

func (p *pageType) Clear() {
	*p = (*p)[:0]
}

func (p pageType) IsFull() bool {
	return len(p) == cap(p)
}

func (p pageType) StoreIndexToRecord(idx int, record *Record) {
	record.TimeStamp = p[idx].TimeStamp
	record.setValue(p[idx].Value)
}

func (p pageType) FindGreaterOrEqual(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp >= ts })
}

func (p pageType) FindGreater(ts float64) int {
	return sort.Search(
		len(p),
		func(idx int) bool { return p[idx].TimeStamp > ts })
}

// Meta data for page
type pageMetaDataType struct {
	seqNo uint64
	owner pageOwnerType
}

func (m *pageMetaDataType) SetSeqNo(i uint64) {
	m.seqNo = i
}

func (m *pageMetaDataType) SeqNo() uint64 {
	return m.seqNo
}

// Represents an actual page in scotty. These pages can either hold timestmps
// value pairs or just timestamps.
// These pages implement github.com/google/btree.Item
type pageWithMetaDataType struct {
	// page queue lock protects this.
	pageMetaDataType
	// Lock of current page owner protects these.
	values []tsValueType
}

func newPageWithMetaDataType(bytesPerPage int) *pageWithMetaDataType {
	return &pageWithMetaDataType{
		values: make(pageType, 0, bytesPerPage/gTsAndValueSize)}
}

// As timestamp value pairs
func (p *pageWithMetaDataType) Values() *pageType {
	return (*pageType)(&p.values)
}

// As timestamps
func (p *pageWithMetaDataType) Times() *tsPageType {
	return (*tsPageType)(&p.values)
}

// As timestamp value pairs
func (p *pageWithMetaDataType) ValuePage() basicPageType {
	return p.Values()
}

// As timestamps
func (p *pageWithMetaDataType) TimePage() basicPageType {
	return p.Times()
}

// github.com/google/btree.Item
func (p *pageWithMetaDataType) Less(than btree.Item) bool {
	pthan := than.(*pageWithMetaDataType)
	return p.seqNo < pthan.seqNo
}

// Fetch iterates over page data p from time start to time end.
// Fetch adds the data to result in descending order by time starting
// just before end and ending on or before start.
// start and end are seconds after Jan 1 1970.
// If caller should add more data from previous pages because caller is not
// back far enough in time, Fetch returns true. Otherwise Fetch returns false.
func Fetch(
	p basicPageType,
	start, end float64,
	record *Record,
	result Appender) (keepGoing bool) {
	lastIdx := p.FindGreaterOrEqual(end)
	if lastIdx == 0 {
		return true
	}
	firstIdx := p.FindGreater(start) - 1
	if firstIdx < 0 {
		keepGoing = true
		firstIdx = 0
	}
	for i := lastIdx - 1; i >= firstIdx; i-- {
		p.StoreIndexToRecord(i, record)
		if !result.Append(record) {
			return false
		}
	}
	return
}

// FetchForward works like Fetch but adds data to result in ascending order
// by time. If caller should continue to subsequent pages FetchForward returns
// true.
// Unlike Fetch where caller can start at the last page, it is the caller's
// responsibility to back up to the right page before calling FetchForward for
// the first time.
func FetchForward(
	p basicPageType,
	start, end float64,
	record *Record,
	result Appender) (keepGoing bool) {
	firstIdx := p.FindGreater(start) - 1
	if firstIdx < 0 {
		firstIdx = 0
	}
	lastIdx := p.FindGreaterOrEqual(end)
	if lastIdx == p.Len() {
		keepGoing = true
	}
	for i := firstIdx; i < lastIdx; i++ {
		p.StoreIndexToRecord(i, record)
		if !result.Append(record) {
			return false
		}
	}
	return
}
