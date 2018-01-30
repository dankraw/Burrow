package notifier

import (
	"testing"
	"fmt"
	"github.com/stretchr/testify/assert"
	"time"
	"go.uber.org/zap"
)

func TestMetricsMap_PutAndGet(t *testing.T) {
	t.Parallel()

	// given
	m := NewMetricsMap(1, zap.NewNop())
	entry := &MetricEntry{
		name: "abc",
	}

	// when
	m.Put(entry)

	// then
	assert.Equal(t, m.Get("abc"), entry)
}

func TestMetricsMap_Get(t *testing.T) {
	t.Parallel()

	// given
	m := NewMetricsMap(1, zap.NewNop())

	// expect
	assert.Nil(t, m.Get("not_exists"))
}

func TestMetricsMap_PutAndIterate(t *testing.T) {
	t.Parallel()

	// given
	m := NewMetricsMap(1, zap.NewNop())
	existing := make(map[string]*MetricEntry)
	var found []*MetricEntry
	size := 75

	// when
	i := size
	for i > 0 {
		name := fmt.Sprintf("item%d", i)
		entry := &MetricEntry{
			name: name,
		}
		existing[name] = entry
		m.Put(entry)
		i -= 1
	}

	// then
	items, done := m.ChanIterator()
	defer close(done)
	for e := range items {
		found = append(found, e)
		if f, ok := existing[e.name]; ok {
			assert.Equal(t, f, e)
		} else {
			t.Error(fmt.Sprintf("Element %v not found in input map", e.name))
		}
	}
	assert.Equal(t, len(found), len(existing))
}

func TestMetricsMap_PutAndClean(t *testing.T) {
	t.Parallel()

	// given
	m := NewMetricsMap(10, zap.NewNop())
	now := time.Now().Unix()

	// when
	m.Put(&MetricEntry{
		name: "a",
		timestamp: now - 15,
	})
	m.Put(&MetricEntry{
		name: "b",
		timestamp: now,
	})
	m.Put(&MetricEntry{
		name: "c",
		timestamp: now - 11,
	})

	// and
	m.Cleanup()

	// then
	assert.Equal(t, 1, m.Len())
	assert.Nil(t, m.Get("a"))
	assert.NotNil(t, m.Get("b"))
	assert.Nil(t, m.Get("c"))
}

func TestMetricsMap_ReleaseLockOnConsumerFailure(t *testing.T) {
	// given
	m := NewMetricsMap(10, zap.NewNop())
	size := 75

	i := size
	for i > 0 {
		name := fmt.Sprintf("item%d", i)
		entry := &MetricEntry{
			name: name,
		}
		m.Put(entry)
		i -= 1
	}

	// when obtaining iterator lock is obtained
	items, done := m.ChanIterator()

	// then another client is halted when trying to put another item
	go func() {
		m.Put(&MetricEntry{
			name: "a",
		})
	}()

	// when the client releases the lock (even though didn't iterate through all the items)
	for range items {
		close(done)
		break
	}

	// another client successfully puts another entry
	assert.NotNil(t, m.Get("a"))
}
