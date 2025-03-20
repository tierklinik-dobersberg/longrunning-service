package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bufbuild/connect-go"
	eventsv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/events/v1"
	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1/longrunningv1connect"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/config"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/manager"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/repo"
	"google.golang.org/protobuf/types/known/anypb"
)

type Service struct {
	longrunningv1connect.UnimplementedLongRunningServiceHandler

	repo      *repo.Repo
	providers *config.Providers
	mng       *manager.Manager

	l        sync.RWMutex
	watchers map[string][]chan *longrunningv1.Operation
}

func New(providers *config.Providers, mng *manager.Manager) *Service {
	svc := &Service{
		repo:      providers.Repo,
		providers: providers,
		mng:       mng,
		watchers:  make(map[string][]chan *longrunningv1.Operation),
	}

	mng.OnLost(svc.notifyWatchers)

	return svc
}

func (s *Service) RegisterOperation(ctx context.Context, req *connect.Request[longrunningv1.RegisterOperationRequest]) (*connect.Response[longrunningv1.RegisterOperationResponse], error) {
	id, authCode, err := s.repo.RegisterOperation(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	op, err := s.repo.GetOperation(ctx, &longrunningv1.GetOperationRequest{
		UniqueId: id,
	})
	if err != nil {
		return nil, err
	}

	if s.providers.EventService != nil {
		go func() {
			anypb, err := anypb.New(op)
			if err != nil {
				slog.Error("failed to convert longrunningv1.Operation to anypb.Any", "error", err)
			} else {
				if _, err := s.providers.EventService.Publish(context.Background(), connect.NewRequest(&eventsv1.Event{
					Event: anypb,
				})); err != nil {
					slog.Error("failed to publish operation to events-service", "error", err)
				}
			}
		}()
	}

	return connect.NewResponse(&longrunningv1.RegisterOperationResponse{
		Operation: op,
		AuthToken: authCode,
	}), nil
}

func (s *Service) UpdateOperation(ctx context.Context, req *connect.Request[longrunningv1.UpdateOperationRequest]) (*connect.Response[longrunningv1.Operation], error) {
	op, err := s.repo.UpdateOperation(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	s.notifyWatchers(op)

	return connect.NewResponse(op), nil
}

func (s *Service) CompleteOperation(ctx context.Context, req *connect.Request[longrunningv1.CompleteOperationRequest]) (*connect.Response[longrunningv1.Operation], error) {
	op, err := s.repo.CompleteOperation(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	s.notifyWatchers(op)

	return connect.NewResponse(op), nil
}

func (s *Service) GetOperation(ctx context.Context, req *connect.Request[longrunningv1.GetOperationRequest]) (*connect.Response[longrunningv1.Operation], error) {
	op, err := s.repo.GetOperation(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(op), nil
}

func (s *Service) QueryOperations(ctx context.Context, req *connect.Request[longrunningv1.QueryOperationsRequest]) (*connect.Response[longrunningv1.QueryOperationsResponse], error) {
	op, err := s.repo.QueryOperations(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&longrunningv1.QueryOperationsResponse{
		Operation:  op,
		TotalCount: int64(len(op)),
	}), nil
}

func (s *Service) WatchOperation(ctx context.Context, req *connect.Request[longrunningv1.GetOperationRequest], stream *connect.ServerStream[longrunningv1.Operation]) error {
	ch := s.addWatcher(req.Msg.UniqueId)
	defer s.removeWatcher(req.Msg.UniqueId, ch)

	for {
		select {
		case update, ok := <-ch:
			// channel get's closed when it's state is set to complete or error.
			if !ok {
				return nil
			}

			if err := stream.Send(update); err != nil {
				slog.Error("failed to publish operation update", "error", err, "uniqueId", req.Msg.UniqueId)

				// If sending fails there's no need to return an error to the caller
				return nil
			}

		case <-ctx.Done():
			return nil
		}
	}
}

func (s *Service) notifyWatchers(op *longrunningv1.Operation) {
	// first, publish the operation to the events-service
	if s.providers.EventService != nil {
		anypb, err := anypb.New(op)
		if err != nil {
			slog.Error("failed to convert longrunningv1.Operation to anypb.Any", "error", err)
		} else {
			if _, err := s.providers.EventService.Publish(context.Background(), connect.NewRequest(&eventsv1.Event{
				Event: anypb,
			})); err != nil {
				slog.Error("failed to publish operation to events-service", "error", err)
			}
		}
	}

	s.l.RLock()
	defer s.l.RUnlock()

	for _, w := range s.watchers[op.UniqueId] {
		select {
		case w <- op:
		case <-time.After(time.Second):
			slog.Warn("failed to notify watcher")
		}
	}

	// close all channels if the operation is either completed or lost since no updates are expected/allowed anymore.
	if op.State == longrunningv1.OperationState_OperationState_COMPLETE || op.State == longrunningv1.OperationState_OperationState_LOST {
		go func() {
			s.l.Lock()
			defer s.l.Unlock()

			for _, w := range s.watchers[op.UniqueId] {
				close(w)
			}

			delete(s.watchers, op.UniqueId)
		}()
	}
}

func (s *Service) addWatcher(id string) chan *longrunningv1.Operation {
	ch := make(chan *longrunningv1.Operation, 100)

	s.l.Lock()
	defer s.l.Unlock()

	s.watchers[id] = append(s.watchers[id], ch)

	return ch
}

func (s *Service) removeWatcher(id string, ch chan *longrunningv1.Operation) {
	s.l.Lock()
	defer s.l.Unlock()

	m := make([]chan *longrunningv1.Operation, 0, len(s.watchers[id])-1)

	for _, w := range s.watchers[id] {
		if w == ch {
			continue
		}

		m = append(m, w)
	}

	s.watchers[id] = m
}
