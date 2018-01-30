package notifier

import (
	"regexp"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"fmt"
	"github.com/linkedin/Burrow/core/protocol"
	"net"
	"strings"
	"bufio"
)

type GraphiteNotifier struct {
	App *protocol.ApplicationContext
	Log *zap.Logger

	name           string
	threshold      int
	groupWhitelist *regexp.Regexp
	groupBlacklist *regexp.Regexp

	metricsPrefix      string
	graphiteAddress    string
	graphiteTcpAddress *net.TCPAddr

	reportingInterval      time.Duration
	metricsCleanupInterval time.Duration
	metricsMap             *MetricsMap

	inactivityTimeout    time.Duration
	lastNotificationTime time.Time

	stopChannel chan struct{}
}

func (module *GraphiteNotifier) Configure(name string, configRoot string) {
	module.name = name

	// Set defaults for module-specific configs if needed
	viper.SetDefault(configRoot+".reporting-interval", 30)
	viper.SetDefault(configRoot+".metrics-cleanup-interval", 5*60)
	viper.SetDefault(configRoot+".metrics-ttl", 60*60)
	viper.SetDefault(configRoot+".metrics-prefix", "stats")
	viper.SetDefault(configRoot+".inactivity-timeout", 60)

	module.metricsPrefix = viper.GetString(configRoot + ".metrics-prefix")
	module.graphiteAddress = viper.GetString(configRoot + ".graphite-address")

	tcpAddress, err := net.ResolveTCPAddr("tcp", module.graphiteAddress)
	if err != nil {
		module.Log.Error("Error resolving graphite address.", zap.Error(err))
	}
	module.Log.Info(fmt.Sprintf("Resolved graphite address to: %v:%d", tcpAddress.IP, tcpAddress.Port))
	module.graphiteTcpAddress = tcpAddress

	module.reportingInterval = viper.GetDuration(configRoot+".reporting-interval") * time.Second
	module.metricsCleanupInterval = viper.GetDuration(configRoot+".metrics-cleanup-interval") * time.Second

	module.inactivityTimeout = viper.GetDuration(configRoot+".inactivity-timeout") * time.Second

	metricsTTL := viper.GetInt64(configRoot + ".metrics-ttl")
	module.stopChannel = make(chan struct{})
	module.metricsMap = NewMetricsMap(metricsTTL, module.Log)
}

func (module *GraphiteNotifier) Start() error {
	// Graphite notifier does not have a running component - no start needed
	go func() {
		reportingTimer := time.Tick(module.reportingInterval)
		cleanupTimer := time.Tick(module.metricsCleanupInterval)
		for {
			select {
			case <-module.stopChannel:
				return
			case <-cleanupTimer:
				module.cleanupMetrics()
			case <-reportingTimer:
				module.reportToGraphite()
			}
		}
	}()
	return nil
}

func (module *GraphiteNotifier) Stop() error {
	close(module.stopChannel)
	return nil
}

func (module *GraphiteNotifier) GetName() string {
	return module.name
}

func (module *GraphiteNotifier) GetGroupWhitelist() *regexp.Regexp {
	return module.groupWhitelist
}

func (module *GraphiteNotifier) GetGroupBlacklist() *regexp.Regexp {
	return module.groupBlacklist
}

func (module *GraphiteNotifier) GetLogger() *zap.Logger {
	return module.Log
}

// Used if we want to skip consumer groups based on more than just threshold and whitelist (handled in the coordinator)
func (module *GraphiteNotifier) AcceptConsumerGroup(status *protocol.ConsumerGroupStatus) bool {
	return true
}

func (module *GraphiteNotifier) Notify(status *protocol.ConsumerGroupStatus, eventId string, startTime time.Time, stateGood bool) {

	module.lastNotificationTime = time.Now()
	now := module.lastNotificationTime.Unix()

	for _, p := range status.Partitions {
		if p.End == nil {
			continue // not containing offsets
		}
		metricName := module.partitionMetricName(status.Cluster, p.Topic, status.Group, p.Partition)
		module.metricsMap.Put(&MetricEntry{
			name:      metricName,
			lag:       p.End.Lag,
			offset:    p.End.Offset,
			timestamp: now,
		})
	}
}

func (module *GraphiteNotifier) cleanupMetrics() {
	module.metricsMap.Cleanup()
}

func (module *GraphiteNotifier) reportToGraphite() {
	if module.lastNotificationTime.Add(module.inactivityTimeout).Unix() < time.Now().Unix() {
		return
	}
	conn, err := net.DialTCP("tcp", nil, module.graphiteTcpAddress)
	if err != nil {
		module.Log.Error("Error while creating connection to graphite.", zap.Error(err))
		return
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	defer writer.Flush()

	now := time.Now().Unix()

	items, done := module.metricsMap.ChanIterator()
	defer close(done)

	for m := range items {
		_, err := fmt.Fprintf(writer, "%s.%s.lag %d %d\n", module.metricsPrefix, m.name, m.lag, now)
		if err != nil {
			module.Log.Error("Error while writing to graphite.", zap.Error(err))
			return
		}
		_, err = fmt.Fprintf(writer, "%s.%s.offset %d %d\n", module.metricsPrefix, m.name, m.offset, now)
		if err != nil {
			module.Log.Error("Error while writing to graphite.", zap.Error(err))
			return
		}
	}
}

func (module *GraphiteNotifier) partitionMetricName(cluster string, topic string, group string, partition int32) string {
	return fmt.Sprintf("%s.%s.%s.%d", module.escapeDots(cluster), module.escapeDots(topic), module.escapeDots(group), partition)
}
func (module *GraphiteNotifier) escapeDots(toEscape string) string {
	return strings.Replace(toEscape, ".", "_", -1)
}
