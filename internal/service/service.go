package service

import (
	"context"

	"github.com/bufbuild/connect-go"
	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1/longrunningv1connect"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/config"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/repo"
)

type Service struct {
	longrunningv1connect.UnimplementedLongRunningServiceHandler

	repo      *repo.Repo
	providers *config.Providers
}

func New(providers *config.Providers) *Service {
	return &Service{
		repo:      providers.Repo,
		providers: providers,
	}
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

	return connect.NewResponse(op), nil
}

func (s *Service) CompleteOperation(ctx context.Context, req *connect.Request[longrunningv1.CompleteOperationRequest]) (*connect.Response[longrunningv1.Operation], error) {
	op, err := s.repo.CompleteOperation(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

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
