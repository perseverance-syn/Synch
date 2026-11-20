package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/execdriver"
)

type fixtureLauncher struct {
	version          string
	transport        *fixtureTransport
	launchErr        error
	probes, launches int
}

// Version returns prerecorded CLI version output.
func (l *fixtureLauncher) Version(context.Context, engine.StartRequest) (string, error) {
	l.probes++
	return l.version, nil
}

// Launch supplies the simulated app-server transport.
func (l *fixtureLauncher) Launch(context.Context, engine.StartRequest) (Transport, error) {
	l.launches++
	return l.transport, l.launchErr
}

type fixtureTransport struct {
	net.Conn
	server net.Conn
	exited chan struct{}
	once   sync.Once
}

// Progress is a no-op because the fake transport has no stall timer.
func (*fixtureTransport) Progress() {}

// Cancel records the simulated supervisor exit and closes the child end.
func (t *fixtureTransport) Cancel(context.Context) error {
	t.once.Do(func() {
		_ = t.server.Close()
		close(t.exited)
	})
	return nil
}

// Wait returns only after the fake supervisor has recorded an exit.
func (t *fixtureTransport) Wait(ctx context.Context) (execdriver.ExitInfo, error) {
	select {
	case <-t.exited:
		return execdriver.ExitInfo{ExecutionOutcome: "cancelled", DescendantsCleaned: true}, nil
	case <-ctx.Done():
		return execdriver.ExitInfo{}, ctx.Err()
	}
}

// cancelled reports whether the adapter requested simulated supervisor cleanup.
func (t *fixtureTransport) cancelled() bool {
	select {
	case <-t.exited:
		return true
	default:
		return false
	}
}

// replay connects a synthetic JSONL peer and checks each expected message during the test.
func replay(t *testing.T, name string, allowEarlyClose ...bool) *fixtureLauncher {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name + ".transcript")
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	if err := server.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	transport := &fixtureTransport{Conn: client, server: server, exited: make(chan struct{})}
	finished := make(chan error, 1)
	go func() { finished <- exchangeTranscript(server, string(data)) }()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		err := <-finished
		expectedClose := len(allowEarlyClose) > 0 && allowEarlyClose[0] && errors.Is(err, net.ErrClosed)
		if err != nil && !expectedClose {
			t.Error(err)
		}
	})
	return &fixtureLauncher{version: "codex-cli 0.169.0", transport: transport}
}

// exchangeTranscript checks client frames and writes prerecorded server frames in order.
func exchangeTranscript(server net.Conn, transcript string) error {
	reader := bufio.NewReader(server)
	for line, text := range strings.Split(strings.TrimSpace(transcript), "\n") {
		text = strings.TrimSuffix(text, "\r")
		if len(text) < 3 {
			return fmt.Errorf("invalid transcript line %d", line+1)
		}
		if text[0] == '<' {
			if _, err := fmt.Fprintln(server, text[2:]); err != nil {
				return fmt.Errorf("write fixture line %d: %w", line+1, errors.Join(net.ErrClosed, err))
			}
			continue
		}
		actual, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read fixture line %d: %w", line+1, err)
		}
		var got, want any
		if err := json.Unmarshal([]byte(actual), &got); err != nil {
			return fmt.Errorf("decode client line %d: %w", line+1, err)
		}
		if err := json.Unmarshal([]byte(text[2:]), &want); err != nil {
			return fmt.Errorf("decode fixture line %d: %w", line+1, err)
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("line %d: got %s want %s", line+1, actual, text[2:])
		}
	}
	return nil
}
