package opentsdb

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/storage"
	"github.com/whitesmith/go-opentsdb"
	"strconv"
)

// Series names
const (
	// Cumulative CPU usage
	serCpuUsageTotal  string = "cpu_usage_total"
	serCpuUsageSystem string = "cpu_usage_system"
	serCpuUsageUser   string = "cpu_usage_user"
	serCpuUsagePerCpu string = "cpu_usage_per_cpu"
	// Smoothed average of number of runnable threads x 1000.
	serLoadAverage string = "load_average"
	// Memory Usage
	serMemoryUsage string = "memory_usage"
	// Working set size
	serMemoryWorkingSet string = "memory_working_set"
	// Cumulative count of bytes received.
	serRxBytes string = "rx_bytes"
	// Cumulative count of receive errors encountered.
	serRxErrors string = "rx_errors"
	// Cumulative count of bytes transmitted.
	serTxBytes string = "tx_bytes"
	// Cumulative count of transmit errors encountered.
	serTxErrors string = "tx_errors"
	// Filesystem device.
	serFsDevice string = "fs_device"
	// Filesystem limit.
	serFsLimit string = "fs_limit"
	// Filesystem usage.
	serFsUsage string = "fs_usage"
)

type openTSDBStorage struct {
	machineName    string
	dbname         string
	client         *opentsdb.Client
	bufferDuration time.Duration
	points         []opentsdb.Point
	lastWrite      time.Time
	lock           sync.Mutex
	readyToFlush   func() bool
}

func init() {
	storage.RegisterStorageDriver("opentsdb", new)
}

func new() (storage.StorageDriver, error) {
	machineName, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	parts := strings.Split(*storage.ArgDbHost, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid db host: %s", *storage.ArgDbHost)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	tsdbClient, err := opentsdb.NewClient(opentsdb.Options{
		Host:    parts[0],
		Port:    port,
		Timeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	storage := &openTSDBStorage{
		machineName:    machineName,
		dbname:         *storage.ArgDbName,
		client:         tsdbClient,
		bufferDuration: *storage.ArgDbBufferDuration,
		points:         make([]opentsdb.Point, 0),
		lastWrite:      time.Now(),
	}
	storage.readyToFlush = storage.defaultReadyToFlush
	return storage, nil
}

func (self *openTSDBStorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	if stats == nil {
		return nil
	}
	var pointsToFlush []opentsdb.Point
	func() {
		// AddStats will be invoked simultaneously from multiple threads and only one of them will perform a write.
		self.lock.Lock()
		defer self.lock.Unlock()

		self.points = append(self.points, self.containerStatsToPoints(ref, stats)...)
		self.points = append(self.points, self.containerFilesystemStatsToPoints(ref, stats)...)
		if self.readyToFlush() {
			pointsToFlush = self.points
			self.points = make([]opentsdb.Point, 0)
			self.lastWrite = time.Now()
		}
	}()
	if len(pointsToFlush) > 0 {
		points := make([]*opentsdb.Point, len(pointsToFlush))
		for i, p := range pointsToFlush {
			points[i] = &p
		}
		code, body, err := self.client.Put(&opentsdb.BatchPoints{points}, "detail")
		if err != nil || (code != 200 && code != 204) {
			return fmt.Errorf("failed to write stats to openTSDB - %v %d %s", err, code, string(body))
		}
	}
	return nil
}

func (self *openTSDBStorage) defaultReadyToFlush() bool {
	return time.Since(self.lastWrite) >= self.bufferDuration
}

func (self *openTSDBStorage) containerStatsToPoints(ref info.ContainerReference, stats *info.ContainerStats) (points []opentsdb.Point) {
	tags := make(map[string]string)
	for k, v := range ref.Labels {
		tags[k] = v
	}
	tags["machine"] = self.machineName
	// cluster-queue-user-machine-app-instance
	points = append(points, makePoint(serCpuUsageTotal, stats.Cpu.Usage.Total, stats.Timestamp, tags))
	// CPU usage: Time spend in system space (in nanoseconds)
	points = append(points, makePoint(serCpuUsageSystem, stats.Cpu.Usage.System, stats.Timestamp, tags))
	// CPU usage: Time spent in user space (in nanoseconds)
	points = append(points, makePoint(serCpuUsageUser, stats.Cpu.Usage.User, stats.Timestamp, tags))
	// CPU usage per CPU
	for i := 0; i < len(stats.Cpu.Usage.PerCpu); i++ {
		points = append(points, makePoint(serCpuUsagePerCpu, stats.Cpu.Usage.PerCpu[i], stats.Timestamp, tags))
	}
	// Load Average
	points = append(points, makePoint(serLoadAverage, stats.Cpu.LoadAverage, stats.Timestamp, tags))
	// Memory Usage
	points = append(points, makePoint(serMemoryUsage, stats.Memory.Usage, stats.Timestamp, tags))
	// Working Set Size
	points = append(points, makePoint(serMemoryWorkingSet, stats.Memory.WorkingSet, stats.Timestamp, tags))
	// Network Stats
	points = append(points, makePoint(serRxBytes, stats.Network.RxBytes, stats.Timestamp, tags))
	points = append(points, makePoint(serRxErrors, stats.Network.RxErrors, stats.Timestamp, tags))
	points = append(points, makePoint(serTxBytes, stats.Network.TxBytes, stats.Timestamp, tags))
	points = append(points, makePoint(serTxErrors, stats.Network.TxErrors, stats.Timestamp, tags))
	return points
}

func (self *openTSDBStorage) containerFilesystemStatsToPoints(ref info.ContainerReference, stats *info.ContainerStats) (points []opentsdb.Point) {
	if len(stats.Filesystem) == 0 {
		return points
	}
	tags := make(map[string]string)
	for k, v := range ref.Labels {
		tags[k] = v
	}
	tags["machine"] = self.machineName
	for _, fsStat := range stats.Filesystem {
		points = append(points, makePoint(serFsUsage, int64(fsStat.Usage), stats.Timestamp, tags))
		points = append(points, makePoint(serFsLimit, int64(fsStat.Limit), stats.Timestamp, tags))
	}
	return points
}

func makePoint(name string, value interface{}, timestamp time.Time, tags map[string]string) opentsdb.Point {
	return opentsdb.Point{
		Metric:    name,
		Value:     value,
		Timestamp: timestamp.Unix(),
		Tags:      tags,
	}
}

func (self *openTSDBStorage) Close() error {
	return nil
}
