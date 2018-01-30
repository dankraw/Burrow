package notifier

import (
	"testing"
	"github.com/linkedin/Burrow/core/protocol"
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"bufio"
	"strings"
	"strconv"
	"go.uber.org/zap"
	"github.com/spf13/viper"
	"time"
)

func TestGraphiteNotifier_ImplementsModule(t *testing.T) {
	assert.Implements(t, (*protocol.Module)(nil), new(GraphiteNotifier))
	assert.Implements(t, (*Module)(nil), new(GraphiteNotifier))
}

func TestGraphiteNotification(t *testing.T) {
	t.Parallel()

	// given
	addr, metrics, metricsWaiter := graphiteServer(t)
	metricsWaiter.Add(1)

	notifier := fixtureGraphiteNotifier(addr)
	notifier.Configure("test", "notifier.test")
	notifier.Start()

	status := &protocol.ConsumerGroupStatus{
		Status:  protocol.StatusOK,
		Cluster: "testcluster",
		Group:   "testgroup",
		Partitions: []*protocol.PartitionStatus{{
			Topic:     "some-topic",
			Partition: 0,
			End: &protocol.ConsumerOffset{
				Lag:    2,
				Offset: 123,
			},
		}, {
			Partition: 1,
			Topic:     "some-topic",
			End: &protocol.ConsumerOffset{
				Lag:    3,
				Offset: 321,
			},
		}},
	}

	// when
	notifier.Notify(status, "some-event", time.Now(), true)

	// then
	metricsWaiter.Wait()

	assert.Equal(t, int64(2), metrics["stats.testcluster.some-topic.testgroup.0.lag"])
	assert.Equal(t, int64(123), metrics["stats.testcluster.some-topic.testgroup.0.offset"])
	assert.Equal(t, int64(3), metrics["stats.testcluster.some-topic.testgroup.1.lag"])
	assert.Equal(t, int64(321), metrics["stats.testcluster.some-topic.testgroup.1.offset"])
}

func fixtureGraphiteNotifier(graphiteAddr *net.TCPAddr) *GraphiteNotifier {
	module := GraphiteNotifier{
		Log: zap.NewNop(),
		App: &protocol.ApplicationContext{},
	}

	viper.Reset()
	viper.Set("notifier.test.class-name", "graphite")
	viper.Set("notifier.test.reporting-interval", 1)
	viper.Set("notifier.test.graphite-address", graphiteAddr.String())

	return &module
}

func graphiteServer(t *testing.T) (*net.TCPAddr, map[string]int64, *sync.WaitGroup) {
	metrics := make(map[string]int64)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("Error while listening on localhost", err)
	}

	var waiter sync.WaitGroup
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal("Error while accepting connection", err)
			}

			r := bufio.NewReader(conn)
			line, err := r.ReadString('\n')

			for err == nil {
				parts := strings.Split(line, " ")
				i, _ := strconv.ParseInt(parts[1], 10, 64)

				metrics[parts[0]] = metrics[parts[0]] + i
				line, err = r.ReadString('\n')
			}
			waiter.Done()
			conn.Close()
		}
	}()

	addr, err := net.ResolveTCPAddr("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal("Error while resolving address", err)
	}
	return addr, metrics, &waiter
}
