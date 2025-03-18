package manager

import (
	"context"
	"log/slog"
	"sync"
	"time"

	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"google.golang.org/protobuf/proto"
)

type (
	// TickerFactory is a function that creates a new time.Ticker.
	// This is mainly used for testing the manager implementation and
	// defaults to time.NewTicker
	TickerFactory func(time.Duration) *time.Ticker

	// SinceFunc is a function that returns how mutch time has elapsed between
	// now and the provided time value. This is mainy for testing the manager
	// implementation and defaults to time.Since
	SinceFunc func(time.Time) time.Duration

	// Repository is the interface required by the manager to query and mark active operations
	// as lost.
	Repository interface {
		// GetActiveOperations should return all operations that are in state RUNNING.
		GetActiveOperations(context.Context) ([]*longrunningv1.Operation, error)

		// MarkAsLost marks an operation as lost by updating it's state to LOST.
		MarkAsLost(context.Context, string) (*longrunningv1.Operation, error)
	}

	Manager struct {
		r             Repository
		wg            sync.WaitGroup
		startOnce     sync.Once
		tickerFactory TickerFactory
		sinceFunc     SinceFunc

		l      sync.RWMutex
		onLost []func(*longrunningv1.Operation)
	}
)

// New returns a new manager that will watch active operations in r and eventually mark them
// as lost if no update happens during the specified TTL and GracePeriod of the operation.
// If tickerFactory is not nil, it will be used to create the ticker for the polling interval,
// if nil, time.NewTicker is used.
// If sinceFunc is not nil it will be used to get the amount of time that has ellapsed since
// the last operation update. If nil, time.Since will be used.
func New(r Repository, tickerFactory TickerFactory, sinceFunc SinceFunc) *Manager {
	if tickerFactory == nil {
		tickerFactory = time.NewTicker
	}

	if sinceFunc == nil {
		sinceFunc = time.Since
	}

	return &Manager{
		r:             r,
		tickerFactory: tickerFactory,
		sinceFunc:     sinceFunc,
	}
}

// OnLost registers a callback function that will be invoked in a separate
// goroutine whenever an operation is marked as lost.
// The operation passed when fn is called is cloned and not shared with any
// other so it's save to manipulate it.
func (m *Manager) OnLost(fn func(*longrunningv1.Operation)) {
	m.l.Lock()
	defer m.l.Unlock()

	m.onLost = append(m.onLost, fn)
}

// Start starts watching active operations.
// If calleds multiple times, Start is a no-op.
//
// To stop a running manager, cancel the context and
// call Wait().
func (m *Manager) Start(ctx context.Context) error {
	m.startOnce.Do(func() {
		m.wg.Add(1)
		ticker := m.tickerFactory(time.Second * 30)

		go func() {
			defer m.wg.Done()
			defer ticker.Stop()

			for {

				m.checkOperations(ctx)

				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	})

	return nil
}

func (m *Manager) checkOperations(ctx context.Context) {
	ops, err := m.r.GetActiveOperations(ctx)
	if err != nil {
		slog.Error("failed to query active operations", "error", err)
		return
	}

	// check each active operation
	for _, op := range ops {
		lastUpdate := op.LastUpdate.AsTime()

		diff := m.sinceFunc(lastUpdate)
		if diff >= (op.Ttl.AsDuration() + op.GracePeriod.AsDuration()) {
			if _, err := m.r.MarkAsLost(ctx, op.UniqueId); err != nil {
				slog.Error("failed to mark operation as lost", "id", op.UniqueId, "description", op.Description, "error", err)
			} else {
				slog.Info("operation lost", "id", op.UniqueId, "description", op.Description)

				m.notifyLost(op)
			}
		}
	}
}

func (m *Manager) notifyLost(op *longrunningv1.Operation) {
	m.l.RLock()
	defer m.l.RUnlock()

	for _, fn := range m.onLost {
		go fn(proto.Clone(op).(*longrunningv1.Operation))
	}
}

// Wait waits for the manager to stop.
// This does not wait for any outstanding OnLost callbacks.
func (m *Manager) Wait() {
	m.wg.Wait()
}
