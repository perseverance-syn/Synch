package remote

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// TestIdleSSHReadsExpire proves R1 expires idle handshakes and authenticated subsystem reads.
func TestIdleSSHReadsExpire(t *testing.T) {
	f := openFixture(t)
	var idle []net.Conn
	for range 5 {
		conn, err := net.Dial("tcp", f.target.address())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		idle = append(idle, conn)
	}
	client, err := dialSSH(testContext(t), f.target, f.broker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	session, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := session.RequestSubsystem(subsystem); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, stdout); done <- err }()
	deadline := time.Now().Add(4 * time.Second)
	for _, conn := range idle {
		if err := conn.SetReadDeadline(deadline); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, conn); err != nil {
			t.Fatalf("idle TCP connection remained open: %v", err)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Until(deadline)):
		t.Fatal("idle subsystem session remained open")
	}
	if _, _, err := callSSH(testContext(t), client, f.runner.id, wireRequest{Operation: OpReplay}); err != nil {
		t.Fatal(err)
	}
}

// TestSSHResponseSizeLimit proves O3 stops reading an oversized JSON response at one MiB.
func TestSSHResponseSizeLimit(t *testing.T) {
	const prefix = `{"runner":"`
	const suffix = `"}`
	within := prefix + strings.Repeat("r", wireMessageLimit-len(prefix)-len(suffix)) + suffix
	if _, err := decodeResponse(strings.NewReader(within)); err != nil {
		t.Fatalf("response at the limit: %v", err)
	}
	source := strings.NewReader(prefix + strings.Repeat("r", wireMessageLimit) + suffix)
	before := source.Len()
	if _, err := decodeResponse(source); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("oversized response: %v", err)
	}
	if read := before - source.Len(); read != wireMessageLimit {
		t.Fatalf("read %d bytes, want %d", read, wireMessageLimit)
	}
}
