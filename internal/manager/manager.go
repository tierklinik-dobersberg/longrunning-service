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

func (m *Manager) OnLost(fn func(*longrunningv1.Operation)) {
	m.l.Lock()
	defer m.l.Unlock()

	m.onLost = append(m.onLost, fn)
}

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

func (m *Manager) Wait() {
	m.wg.Wait()
}
