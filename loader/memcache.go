package loader

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"time"
)

type writer interface {
	Set(key string, value []byte) error
	Close() error
}

type writerFactory func(address string, timeout time.Duration) (writer, error)

type memcacheWriter struct {
	conn    net.Conn
	reader  *bufio.Reader
	timeout time.Duration
}

func newMemcacheWriter(address string, timeout time.Duration) (writer, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	return &memcacheWriter{conn: conn, reader: bufio.NewReader(conn), timeout: timeout}, nil
}

func (client *memcacheWriter) Set(key string, value []byte) error {
	if err := client.conn.SetDeadline(time.Now().Add(client.timeout)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(client.conn, "set %s 0 0 %d\r\n", key, len(value)); err != nil {
		return err
	}
	if _, err := client.conn.Write(value); err != nil {
		return err
	}
	if _, err := io.WriteString(client.conn, "\r\n"); err != nil {
		return err
	}
	response, err := client.reader.ReadString('\n')
	if err != nil {
		return err
	}
	if response != "STORED\r\n" {
		return fmt.Errorf("unexpected memcached response %q", response)
	}
	return nil
}

func (client *memcacheWriter) Close() error { return client.conn.Close() }
