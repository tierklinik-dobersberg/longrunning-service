package op

import (
	"context"
	"fmt"
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

type Option func(req *connect.Request[longrunningv1.RegisterOperationRequest])

func Wrap[T any](ctx context.Context, cli longrunningv1connect.LongRunningServiceClient, fn func(ctx context.Context) (T, error), ops ...Option) (T, error) {
	var empty T

	req := connect.NewRequest(&longrunningv1.RegisterOperationRequest{
		InitialState: longrunningv1.OperationState_OperationState_RUNNING,
	})

	for _, op := range ops {
		op(req)
	}

	// clone the request headers since we need them for updating/completing as well.
	headers := req.Header().Clone()

	res, err := cli.RegisterOperation(ctx, req)
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

			updReq := connect.NewRequest(&longrunningv1.UpdateOperationRequest{
				UniqueId:  res.Msg.Operation.UniqueId,
				AuthToken: res.Msg.GetAuthToken(),
				Running:   true,
				UpdateMask: &fieldmaskpb.FieldMask{
					Paths: []string{"running"},
				},
			})

			for key, values := range headers {
				for _, v := range values {
					updReq.Header().Add(key, v)
				}
			}

			_, err := cli.UpdateOperation(ctx, updReq)
			if err != nil {
				slog.Error("failed to update operation", "error", err)
			}
		}
	}()

	result, resultErr := callAndCatch(func() (T, error) {
		return fn(ctx)
	})
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

	completeRequest := connect.NewRequest(creq)
	for key, values := range headers {
		for _, v := range values {
			completeRequest.Header().Add(key, v)
		}
	}

	if _, err := cli.CompleteOperation(context.Background(), completeRequest); err != nil {
		slog.Error("failed to mark operation as complete", "error", err.Error())
	}

	return result, resultErr
}

func callAndCatch[T any](fn func() (T, error)) (T, error) {

	var resultErr error

	defer func() {
		if x := recover(); x != nil {
			if e, ok := x.(error); ok {
				resultErr = e
			} else {
				resultErr = fmt.Errorf("panic: %v", x)
			}
		}
	}()

	result, resultErr := fn()

	return result, resultErr
}
