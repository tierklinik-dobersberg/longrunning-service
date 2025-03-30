package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	connect "github.com/bufbuild/connect-go"
	"github.com/bufbuild/protovalidate-go"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1/longrunningv1connect"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/typeserver/v1/typeserverv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/apis/pkg/codec"
	"github.com/tierklinik-dobersberg/apis/pkg/cors"
	"github.com/tierklinik-dobersberg/apis/pkg/discovery"
	"github.com/tierklinik-dobersberg/apis/pkg/discovery/consuldiscover"
	"github.com/tierklinik-dobersberg/apis/pkg/discovery/wellknown"
	"github.com/tierklinik-dobersberg/apis/pkg/h2utils"
	"github.com/tierklinik-dobersberg/apis/pkg/log"
	"github.com/tierklinik-dobersberg/apis/pkg/server"
	"github.com/tierklinik-dobersberg/apis/pkg/validator"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/config"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/manager"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/service"
	"github.com/tierklinik-dobersberg/pbtype-server/pkg/resolver"
	"google.golang.org/protobuf/reflect/protoregistry"
)

var serverContextKey = struct{ S string }{S: "serverContextKey"}

type resolverFactors struct {
	catalog discovery.Discoverer
}

func (r resolverFactors) Create() (typeserverv1connect.TypeResolverServiceClient, error) {
	svcs, err := r.catalog.Discover(context.Background(), wellknown.TypeV1ServiceScope)
	if err != nil {
		return nil, err
	}

	if len(svcs) == 0 {
		return nil, fmt.Errorf("no service instances found")
	}

	i := svcs[rand.IntN(len(svcs))]
	addr := fmt.Sprintf("http://%s", i.Address)

	return typeserverv1connect.NewTypeResolverServiceClient(h2utils.NewInsecureHttp2Client(), addr), nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		slog.Error("failed to load configuration", slog.Any("error", err.Error()))
		os.Exit(-1)
	}

	catalog, err := consuldiscover.NewFromEnv()
	if err != nil {
		slog.Error("failed to create service discovery client", slog.Any("error", err.Error()))
		os.Exit(-1)
	}

	protoValidator, err := protovalidate.New()
	if err != nil {
		slog.Error("failed to prepare protovalidate", slog.Any("error", err.Error()))
		os.Exit(-1)
	}

	// TODO(ppacher): privacy-interceptor
	interceptors := connect.WithInterceptors(
		log.NewLoggingInterceptor(),
		validator.NewInterceptor(protoValidator),
	)

	resolver := resolver.WrapFactory(resolverFactors{catalog: catalog}, protoregistry.GlobalFiles, protoregistry.GlobalTypes)

	c := codec.NewCodec(resolver)

	interceptors = connect.WithOptions(interceptors, connect.WithCodec(c))

	if roleClient, err := wellknown.RoleService.Create(ctx, catalog); err == nil {
		authInterceptor := auth.NewAuthAnnotationInterceptor(
			protoregistry.GlobalFiles,
			auth.NewIDMRoleResolver(roleClient),
			func(ctx context.Context, req connect.AnyRequest) (auth.RemoteUser, error) {
				serverKey, _ := ctx.Value(serverContextKey).(string)

				if serverKey == "admin" {
					return auth.RemoteUser{
						ID:          "service-account",
						DisplayName: req.Peer().Addr,
						RoleIDs:     []string{"idm_superuser"},
						Admin:       true,
					}, nil
				}

				return auth.RemoteHeaderExtractor(ctx, req)
			},
		)

		interceptors = connect.WithOptions(interceptors, connect.WithInterceptors(authInterceptor))
	}

	corsConfig := cors.Config{
		AllowedOrigins:   cfg.AllowedOrigins,
		AllowCredentials: true,
	}

	providers, err := cfg.ConfigureProviders(ctx, catalog)
	if err != nil {
		slog.Error("failed to configure providers", slog.Any("error", err.Error()))
		os.Exit(-1)
	}

	// create a new manager that will handle lost operations
	mng := manager.New(providers.Repo, nil, nil)
	if err := mng.Start(ctx); err != nil {
		slog.Error("failed to start manager", "error", err)
		os.Exit(-1)
	}

	svc := service.New(providers, mng)

	serveMux := http.NewServeMux()

	path, handler := longrunningv1connect.NewLongRunningServiceHandler(svc, interceptors)
	serveMux.Handle(path, handler)

	loggingHandler := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			next.ServeHTTP(w, r)

			slog.Info("handled request", slog.Any("method", r.Method), slog.Any("path", r.URL.Path), slog.Any("duration", time.Since(start).String()))
		})
	}

	wrapWithKey := func(key string, next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r = r.WithContext(context.WithValue(r.Context(), serverContextKey, key))

			next.ServeHTTP(w, r)
		})
	}

	// Create the server
	srv, err := server.CreateWithOptions(cfg.ListenAddress, wrapWithKey("public", loggingHandler(serveMux)), server.WithCORS(corsConfig))
	if err != nil {
		slog.Error("failed to setup server", slog.Any("error", err.Error()))
		os.Exit(-1)
	}

	adminSrv, err := server.CreateWithOptions(cfg.AdminListenAddress, wrapWithKey("admin", loggingHandler(serveMux)), server.WithCORS(corsConfig))
	if err != nil {
		slog.Error("failed to setup server", slog.Any("error", err.Error()))
		os.Exit(-1)
	}

	// Enable service discovery
	if err := discovery.Register(ctx, catalog, &discovery.ServiceInstance{
		Name:    wellknown.LongrunningV1ServiceScope,
		Address: cfg.AdminListenAddress,
	}); err != nil {
		slog.Error("failed to register service and service catalog", slog.Any("error", err.Error()))
		os.Exit(-1)
	}

	if err := server.Serve(ctx, srv, adminSrv); err != nil {
		slog.Error("failed to serve", slog.Any("error", err.Error()))
		os.Exit(-1)
	}
}
