package loader

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseLine(t *testing.T) {
	record, err := ParseLine("idfa\tdevice-1\t55.5\t42\t1,bad, 2,-3")
	if err != nil {
		t.Fatal(err)
	}
	if record.DeviceType != "idfa" || record.DeviceID != "device-1" || record.Lat != 55.5 || record.Lon != 42 {
		t.Fatalf("unexpected record: %#v", record)
	}
	if len(record.Apps) != 2 || record.Apps[0] != 1 || record.Apps[1] != 2 {
		t.Fatalf("unexpected apps: %v", record.Apps)
	}
	for _, invalid := range []string{"not\tenough", "idfa\t\t1\t2\t3", "idfa\tid\tbad\t2\t3"} {
		if _, err := ParseLine(invalid); err == nil {
			t.Fatalf("expected %q to fail", invalid)
		}
	}
}

func TestMarshalUserAppsWireFormat(t *testing.T) {
	data := marshalUserApps(Record{Apps: []uint32{1, 7, 300}, Lat: 12.5, Lon: -8.25})
	apps, lat, lon, err := decodeUserApps(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 3 || apps[2] != 300 || lat != 12.5 || lon != -8.25 {
		t.Fatalf("unexpected decoded protobuf: apps=%v lat=%v lon=%v", apps, lat, lon)
	}
}

type fakeWriter struct {
	set   func(string, []byte) error
	close func() error
}

func (writer fakeWriter) Set(key string, value []byte) error { return writer.set(key, value) }
func (writer fakeWriter) Close() error {
	if writer.close != nil {
		return writer.close()
	}
	return nil
}

func TestRunWritesConcurrentlyAndRenamesFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "20240101000000.tsv.gz")
	lines := make([]string, 20)
	for index := range lines {
		lines[index] = "idfa\tdevice-" + string(rune('a'+index)) + "\t1.5\t2.5\t1,42"
	}
	writeGzip(t, path, lines)
	var active atomic.Int32
	var maximum atomic.Int32
	var writes atomic.Int32
	factory := func(address string, timeout time.Duration) (writer, error) {
		return fakeWriter{set: func(_ string, _ []byte) error {
			current := active.Add(1)
			for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
			}
			time.Sleep(5 * time.Millisecond)
			writes.Add(1)
			active.Add(-1)
			return nil
		}}, nil
	}

	results, err := run(testConfig(path), nil, factory)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || !results[0].Successful() || results[0].Processed != 20 {
		t.Fatalf("unexpected results: %#v", results)
	}
	if maximum.Load() < 2 || writes.Load() != 20 {
		t.Fatalf("writes were not concurrent: max=%d writes=%d", maximum.Load(), writes.Load())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "."+filepath.Base(path))); err != nil {
		t.Fatalf("renamed file missing: %v", err)
	}
}

func TestFailedWriteIsRetried(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.tsv.gz")
	writeGzip(t, path, []string{"idfa\tdevice\t1\t2\t3"})
	var attempts atomic.Int32
	factory := func(string, time.Duration) (writer, error) {
		return fakeWriter{set: func(string, []byte) error {
			if attempts.Add(1) < 3 {
				return errors.New("temporary failure")
			}
			return nil
		}}, nil
	}
	results, err := run(testConfig(path), nil, factory)
	if err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 3 || !results[0].Successful() {
		t.Fatalf("attempts=%d result=%#v", attempts.Load(), results[0])
	}
}

func TestFilesAreProcessedInNameOrder(t *testing.T) {
	directory := t.TempDir()
	writeGzip(t, filepath.Join(directory, "20240101000100.tsv.gz"), []string{"idfa\tlater\t1\t2\t3"})
	writeGzip(t, filepath.Join(directory, "20240101000000.tsv.gz"), []string{"idfa\tearlier\t1\t2\t3"})
	var mutex sync.Mutex
	var keys []string
	factory := func(string, time.Duration) (writer, error) {
		return fakeWriter{set: func(key string, _ []byte) error {
			mutex.Lock()
			keys = append(keys, key)
			mutex.Unlock()
			return nil
		}}, nil
	}
	config := testConfig(filepath.Join(directory, "*.tsv.gz"))
	config.Workers = 1
	results, err := run(config, nil, factory)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || keys[0] != "idfa:earlier" || keys[1] != "idfa:later" {
		t.Fatalf("unexpected order: results=%#v keys=%v", results, keys)
	}
}

func TestCorruptGzipIsNotRenamed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.tsv.gz")
	if err := os.WriteFile(path, []byte("not gzip"), 0o600); err != nil {
		t.Fatal(err)
	}
	results, err := run(testConfig(path), nil, func(string, time.Duration) (writer, error) {
		return fakeWriter{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Successful() || results[0].ReadSuccessfully || results[0].Renamed {
		t.Fatalf("unexpected result: %#v", results[0])
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("source was removed: %v", err)
	}
}

func testConfig(pattern string) Config {
	return Config{
		Pattern: pattern, Addresses: map[string]string{"idfa": "memcached:11211"},
		Workers: 4, QueueSize: 4, Retries: 2, Timeout: time.Second, MaxLineBytes: 1024 * 1024,
	}
}

func writeGzip(t *testing.T, path string, lines []string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	compressed := gzip.NewWriter(file)
	for _, line := range lines {
		if _, err := io.WriteString(compressed, line+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func decodeUserApps(data []byte) ([]uint32, float64, float64, error) {
	var apps []uint32
	var lat, lon float64
	for len(data) > 0 {
		tag, bytesRead := binary.Uvarint(data)
		if bytesRead <= 0 {
			return nil, 0, 0, errors.New("bad tag")
		}
		data = data[bytesRead:]
		switch tag {
		case 0x08:
			value, n := binary.Uvarint(data)
			if n <= 0 {
				return nil, 0, 0, errors.New("bad app")
			}
			apps, data = append(apps, uint32(value)), data[n:]
		case 0x11, 0x19:
			if len(data) < 8 {
				return nil, 0, 0, errors.New("bad double")
			}
			value := math.Float64frombits(binary.LittleEndian.Uint64(data))
			if tag == 0x11 {
				lat = value
			} else {
				lon = value
			}
			data = data[8:]
		default:
			return nil, 0, 0, errors.New("unexpected field")
		}
	}
	return apps, lat, lon, nil
}
