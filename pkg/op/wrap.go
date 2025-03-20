package op

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/bufbuild/connect-go"
	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1/longrunningv1connect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

type Option func(req *longrunningv1.RegisterOperationRequest)

func Wrap[T any](ctx context.Context, cli longrunningv1connect.LongRunningServiceClient, fn func(ctx context.Context) (T, error), ops ...Option) (T, error) {
	var empty T

	req := &longrunningv1.RegisterOperationRequest{}

	for _, op := range ops {
		op(req)
	}

	res, err := cli.RegisterOperation(ctx, connect.NewRequest(req))
	if err != nil {
		return empty, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(res.Msg.Operation.Ttl.AsDuration()):
			}

			_, err := cli.UpdateOperation(ctx, connect.NewRequest(&longrunningv1.UpdateOperationRequest{
				UniqueId:  res.Msg.Operation.UniqueId,
				AuthToken: res.Msg.GetAuthToken(),
				Running:   true,
				UpdateMask: &fieldmaskpb.FieldMask{
					Paths: []string{"running"},
				},
			}))
			if err != nil {
				slog.Error("failed to update operation", "error", err)
			}
		}
	}()

	result, resultErr := fn(ctx)
	cancel()

	wg.Wait()

	creq := &longrunningv1.CompleteOperationRequest{
		UniqueId:  res.Msg.GetOperation().GetUniqueId(),
		AuthToken: res.Msg.GetAuthToken(),
	}

	if resultErr == nil {
		var anyv *anypb.Any

		rpb, err := structpb.NewValue(result)
		if err != nil {
			anyv, err = anypb.New(rpb)
		}

		if err != nil {
			slog.Error("failed to conver result for operation", "error", err)
		}

		creq.Result = &longrunningv1.CompleteOperationRequest_Success{
			Success: &longrunningv1.OperationSuccess{
				Result: anyv,
			},
		}
	} else {
		creq.Result = &longrunningv1.CompleteOperationRequest_Error{
			Error: &longrunningv1.OperationError{},
		}
	}

	if _, err := cli.CompleteOperation(context.Background(), connect.NewRequest(creq)); err != nil {
		slog.Error("failed to mark operation as complete", "error", err.Error())
	}

	return result, resultErr
}
