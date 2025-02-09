package config

import (
	"github.com/tierklinik-dobersberg/apis/pkg/discovery"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/repo"
)

type Providers struct {
	*Config

	Repo *repo.Repo

	Catalog discovery.Discoverer
}
