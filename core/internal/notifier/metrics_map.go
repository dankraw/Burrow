package notifier

import (
	"sync"
	"time"
	"go.uber.org/zap"
)

type MetricEntry struct {
	name      string
	lag       uint64
	offset    int64
	timestamp int64
}

type MetricsMap struct {
	metrics map[string]*MetricEntry
	ttl     int64
	lock    sync.Mutex
	log     *zap.Logger
}

func NewMetricsMap(ttl int64, log *zap.Logger) *MetricsMap {
	m := &MetricsMap{
		metrics: make(map[string]*MetricEntry),
		ttl:     ttl,
		log: log,
	}
	return m
}

func (m *MetricsMap) Put(entry *MetricEntry) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.metrics[entry.name] = entry
}

func (m *MetricsMap) Get(name string) *MetricEntry {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.metrics[name]
}

func (m *MetricsMap) Len() int {
	m.lock.Lock()
	defer m.lock.Unlock()

	return len(m.metrics)
}

func (m *MetricsMap) ChanIterator() (<- chan *MetricEntry, chan<- struct{}) {
	var items = make(chan *MetricEntry)
	var done = make(chan struct{})

	go func() {
		m.lock.Lock()
		defer m.lock.Unlock()
		defer close(items)

		for _, metric := range m.metrics {
			select {
				case <-done:
					return
				case items <- metric:
			}
		}
	}()
	return items, done
}

func (m *MetricsMap) Cleanup() {
	m.lock.Lock()
	defer m.lock.Unlock()

	now := time.Now().Unix()
	i := 0
	for name, metric := range m.metrics {
		if metric.timestamp + m.ttl < now {
			delete(m.metrics, name)
			i++
		}
	}
}
