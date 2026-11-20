package remote

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const protocolChunk = 32 << 10

type protocolCapture struct {
	spool *os.File
	stdin io.WriteCloser
	done  chan struct{}
	err   error
}

// prepareProtocol allocates a private spool before spawn if lossless protocol output was requested.
func prepareProtocol(enabled bool) (*protocolCapture, error) {
	if !enabled {
		return nil, nil
	}
	spool, err := os.CreateTemp("", "synch-protocol-")
	if err != nil {
		return nil, fmt.Errorf("open protocol spool: %w", err)
	}
	return &protocolCapture{spool: spool}, nil
}

// start drains protocol stdout for the process lifetime, even while the daemon is offline.
func (p *protocolCapture) start(proc interface {
	ProtocolStreams() (io.WriteCloser, io.ReadCloser)
}) {
	stdin, stdout := proc.ProtocolStreams()
	p.stdin = stdin
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		_, err := io.Copy(p.spool, stdout)
		p.err = errors.Join(err, stdout.Close())
	}()
}

// read returns a bounded range of output and whether the process closed stdout.
func (p *protocolCapture) read(offset int64) (data []byte, finished bool, err error) {
	select {
	case <-p.done:
		finished = true
	default:
	}
	buf := make([]byte, protocolChunk)
	n, err := p.spool.ReadAt(buf, offset)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if n != 0 || !finished {
		return buf[:n], false, err
	}
	return nil, true, errors.Join(err, p.err)
}

// close removes the spool after the supervisor has closed the process pipes.
func (p *protocolCapture) close() error {
	if p.done != nil {
		<-p.done
	}
	return errors.Join(p.spool.Close(), os.Remove(p.spool.Name()))
}
