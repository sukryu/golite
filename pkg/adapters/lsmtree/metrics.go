// 개선된 Metrics 구현 using atomic
package lsmtree

import "sync/atomic"

type Metrics struct {
	Writes    int64
	Reads     int64
	CacheHits int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) IncWrites() {
	atomic.AddInt64(&m.Writes, 1)
}

func (m *Metrics) IncReads() {
	atomic.AddInt64(&m.Reads, 1)
}

func (m *Metrics) IncCacheHit() {
	atomic.AddInt64(&m.CacheHits, 1)
}
