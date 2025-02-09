package service

import (
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
