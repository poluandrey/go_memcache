// Package loader parses tracker logs and concurrently stores protobuf values in memcached.
package loader

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const NormalErrorRate = 0.01

type Record struct {
	DeviceType string
	DeviceID   string
	Lat        float64
	Lon        float64
	Apps       []uint32
}

type Config struct {
	Pattern      string
	Addresses    map[string]string
	Workers      int
	QueueSize    int
	Retries      int
	Timeout      time.Duration
	DryRun       bool
	MaxLineBytes int
}

type Result struct {
	Path             string
	Processed        uint64
	Errors           uint64
	ErrorRate        float64
	ReadSuccessfully bool
	Renamed          bool
}

func (result Result) Successful() bool {
	return result.ReadSuccessfully && result.Renamed && result.ErrorRate < NormalErrorRate
}

type counters struct {
	processed atomic.Uint64
	errors    atomic.Uint64
}

type task struct {
	address string
	record  Record
	stats   *counters
	done    *sync.WaitGroup
}

type pool struct {
	tasks   chan task
	workers sync.WaitGroup
	config  Config
	factory writerFactory
	logger  *slog.Logger
}

func newPool(config Config, factory writerFactory, logger *slog.Logger) *pool {
	p := &pool{tasks: make(chan task, config.QueueSize), config: config, factory: factory, logger: logger}
	p.workers.Add(config.Workers)
	for range config.Workers {
		go p.runWorker()
	}
	return p
}

func (p *pool) submit(item task) {
	item.done.Add(1)
	p.tasks <- item // bounded channel provides backpressure
}

func (p *pool) close() {
	close(p.tasks)
	p.workers.Wait()
}

func (p *pool) runWorker() {
	defer p.workers.Done()
	clients := make(map[string]writer)
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()

	for item := range p.tasks {
		if p.write(item, clients) {
			item.stats.processed.Add(1)
		} else {
			item.stats.errors.Add(1)
		}
		item.done.Done()
	}
}

func (p *pool) write(item task, clients map[string]writer) bool {
	key := item.record.DeviceType + ":" + item.record.DeviceID
	value := marshalUserApps(item.record)
	if p.config.DryRun {
		p.logger.Debug("dry-run write", "address", item.address, "key", key, "bytes", len(value))
		return true
	}

	var lastErr error
	for attempt := 0; attempt <= p.config.Retries; attempt++ {
		client := clients[item.address]
		if client == nil {
			client, lastErr = p.factory(item.address, p.config.Timeout)
			if lastErr == nil {
				clients[item.address] = client
			}
		}
		if client != nil {
			lastErr = client.Set(key, value)
			if lastErr == nil {
				return true
			}
			_ = client.Close()
			delete(clients, item.address)
		}
		if attempt < p.config.Retries {
			p.logger.Warn("memcached write failed; retrying", "address", item.address, "attempt", attempt+1, "error", lastErr)
		}
	}
	p.logger.Error("memcached write failed", "address", item.address, "key", key, "attempts", p.config.Retries+1, "error", lastErr)
	return false
}

func ParseLine(line string) (Record, error) {
	parts := strings.Split(strings.TrimRight(line, "\r\n"), "\t")
	if len(parts) != 5 || parts[0] == "" || parts[1] == "" {
		return Record{}, errors.New("expected five TSV fields with device type and id")
	}
	lat, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return Record{}, fmt.Errorf("parse latitude: %w", err)
	}
	lon, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return Record{}, fmt.Errorf("parse longitude: %w", err)
	}
	record := Record{DeviceType: parts[0], DeviceID: parts[1], Lat: lat, Lon: lon}
	for _, raw := range strings.Split(parts[4], ",") {
		app, parseErr := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
		if parseErr == nil {
			record.Apps = append(record.Apps, uint32(app))
		}
	}
	return record, nil
}

func Run(config Config, logger *slog.Logger) ([]Result, error) {
	return run(config, logger, newMemcacheWriter)
}

func run(config Config, logger *slog.Logger, factory writerFactory) ([]Result, error) {
	if err := validate(config); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	paths, err := filepath.Glob(config.Pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid input pattern: %w", err)
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		logger.Warn("no input files matched", "pattern", config.Pattern)
		return nil, nil
	}

	p := newPool(config, factory, logger)
	defer p.close()
	results := make([]Result, 0, len(paths))
	for _, path := range paths {
		results = append(results, processFile(path, config, p, logger))
	}
	return results, nil
}

func validate(config Config) error {
	if config.Workers < 1 || config.QueueSize < 1 || config.Retries < 0 || config.Timeout <= 0 || config.MaxLineBytes < 1 {
		return errors.New("workers, queue-size, timeout and max-line-bytes must be positive; retries cannot be negative")
	}
	return nil
}

func processFile(path string, config Config, p *pool, logger *slog.Logger) Result {
	logger.Info("processing file", "path", path)
	stats := &counters{}
	var fileTasks sync.WaitGroup
	readOK := readRecords(path, config.MaxLineBytes, func(record Record) {
		address, exists := config.Addresses[record.DeviceType]
		if !exists {
			stats.errors.Add(1)
			logger.Error("unknown device type", "type", record.DeviceType)
			return
		}
		p.submit(task{address: address, record: record, stats: stats, done: &fileTasks})
	}, func(err error) {
		stats.errors.Add(1)
		logger.Debug("invalid input record", "path", path, "error", err)
	})
	fileTasks.Wait() // file barrier preserves input order and safe renaming

	processed, failed := stats.processed.Load(), stats.errors.Load()
	total := processed + failed
	errorRate := 0.0
	if total > 0 {
		errorRate = float64(failed) / float64(total)
	}
	renamed := false
	if readOK {
		destination := filepath.Join(filepath.Dir(path), "."+filepath.Base(path))
		if err := os.Rename(path, destination); err != nil {
			logger.Error("cannot mark file as processed", "path", path, "error", err)
		} else {
			renamed = true
		}
	}
	result := Result{path, processed, failed, errorRate, readOK, renamed}
	logger.Info("file finished", "path", path, "processed", processed, "errors", failed, "error_rate", errorRate)
	return result
}

func readRecords(path string, maxLineBytes int, valid func(Record), invalid func(error)) bool {
	file, err := os.Open(path)
	if err != nil {
		invalid(err)
		return false
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		invalid(err)
		return false
	}
	defer compressed.Close()
	scanner := bufio.NewScanner(compressed)
	scanner.Buffer(make([]byte, 64*1024), maxLineBytes)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		record, parseErr := ParseLine(scanner.Text())
		if parseErr != nil {
			invalid(parseErr)
			continue
		}
		valid(record)
	}
	if err := scanner.Err(); err != nil {
		invalid(err)
		return false
	}
	return true
}
