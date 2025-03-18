package config

import (
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/events/v1/eventsv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/discovery"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/repo"
)

type Providers struct {
	Config *Config

	Repo *repo.Repo

	Catalog discovery.Discoverer

	EventService eventsv1connect.EventServiceClient
}
