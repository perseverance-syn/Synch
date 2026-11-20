package supervisor

import (
	"fmt"
	"os"
)

// startWithStreams attaches pipes before launch and starts draining captured streams.
func (p *Proc) startWithStreams() (err error) {
	p.stdin, err = p.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("create stdin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = p.stdin.Close()
		}
	}()
	stderr, err := p.openStream("stderr")
	if err != nil {
		return err
	}
	captured := []*stream{stderr}
	defer func() {
		for _, stream := range captured {
			if err != nil {
				stream.discard()
			} else {
				_ = stream.writer.Close()
			}
		}
	}()
	p.cmd.Stderr = stderr.writer
	if p.spec.ProtocolIO {
		// An OS pipe preserves bytes and applies backpressure even before the caller starts reading.
		reader, writer, pipeErr := os.Pipe()
		if pipeErr != nil {
			return fmt.Errorf("create protocol stdout: %w", pipeErr)
		}
		defer func() {
			_ = writer.Close()
			if err != nil {
				_ = reader.Close()
			}
		}()
		p.protocolOut, p.cmd.Stdout = reader, writer
	} else {
		stdout, openErr := p.openStream("stdout")
		if openErr != nil {
			return openErr
		}
		captured = append(captured, stdout)
		p.cmd.Stdout = stdout.writer
	}
	p.tree, err = startTree(p.cmd)
	if err != nil {
		return fmt.Errorf("start contained process: %w", err)
	}
	p.readers.Add(len(captured))
	for _, stream := range captured {
		p.pipes = append(p.pipes, stream.reader)
		go stream.drain()
	}
	return nil
}
