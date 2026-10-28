package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/poluandrey/go_memcache/loader"
)

func main() {
	os.Exit(run())
}

func run() int {
	config := loader.Config{Addresses: make(map[string]string)}
	flag.StringVar(&config.Pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "input file glob")
	flag.IntVar(&config.Workers, "workers", runtime.NumCPU()*4, "number of writer goroutines")
	flag.IntVar(&config.QueueSize, "queue-size", 10_000, "bounded write queue size")
	flag.IntVar(&config.Retries, "retries", 2, "retries after the first write attempt")
	flag.DurationVar(&config.Timeout, "timeout", 3*time.Second, "memcached network timeout")
	flag.BoolVar(&config.DryRun, "dry", false, "parse and serialize without writing to memcached")
	flag.IntVar(&config.MaxLineBytes, "max-line-bytes", 1024*1024, "maximum uncompressed input line size")
	idfa := flag.String("idfa", "127.0.0.1:33013", "idfa memcached address")
	gaid := flag.String("gaid", "127.0.0.1:33014", "gaid memcached address")
	adid := flag.String("adid", "127.0.0.1:33015", "adid memcached address")
	dvid := flag.String("dvid", "127.0.0.1:33016", "dvid memcached address")
	verbose := flag.Bool("verbose", false, "enable debug logging")
	flag.Parse()
	config.Addresses = map[string]string{"idfa": *idfa, "gaid": *gaid, "adid": *adid, "dvid": *dvid}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	results, err := loader.Run(config, logger)
	if err != nil {
		logger.Error("loader failed", "error", err)
		return 1
	}
	for _, result := range results {
		if !result.Successful() {
			fmt.Fprintf(os.Stderr, "failed: %s (processed=%d errors=%d error_rate=%.4f)\n", result.Path, result.Processed, result.Errors, result.ErrorRate)
			return 1
		}
	}
	return 0
}
