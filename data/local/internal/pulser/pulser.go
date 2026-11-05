package pulser

import (
	"context"
	"os"
	"sync"
	"time"
)


type Pulse struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
}

// Stops the pulse and waits for goroutine to exit.
func (p *Pulse) Close() {
	p.cancel()
	<-p.done
}

// Closed when pulse goroutine exits.
func (p *Pulse) Done() <-chan struct{} {
	return p.done
}

// Err returns error that stopped the pulse, unless it is still 
// running or was closed early it is nil. 
// Safe to call in any goroutine.
func (p *Pulse) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func Start(path string, interval time.Duration) *Pulse {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	p := &Pulse{
		cancel: cancel,
		done: done,
	}

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := os.Chtimes(path, time.Time{}, time.Now()); err != nil {
					p.mu.Lock()
					p.err = err
					p.mu.Unlock()
					return
				}
			}
		}
	}()

	return p
}
