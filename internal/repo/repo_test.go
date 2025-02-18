package repo_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/mongotest"
	"github.com/tierklinik-dobersberg/longrunning-service/internal/repo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRepository(t *testing.T) {
	ctx, cli := mongotest.Start(t)

	r, err := repo.NewRepoWithClient(ctx, cli, "test-db")
	require.NoError(t, err)

	var (
		id   string
		auth string
	)
	t.Run("RegisterOperation", func(t *testing.T) {
		param1, err := structpb.NewValue("foobar")
		require.NoError(t, err)

		id, auth, err = r.RegisterOperation(ctx, &longrunningv1.RegisterOperationRequest{
			Owner:        "test",
			Creator:      "test-case",
			InitialState: longrunningv1.OperationState_OperationState_RUNNING,
			Ttl:          durationpb.New(time.Minute),
			GracePeriod:  durationpb.New(time.Second),
			Description:  "A simple test operation",
			Parameters: map[string]*structpb.Value{
				"param1": param1,
			},
			Annotations: map[string]string{
				"foo": "bar",
			},
			Kind: "test-op",
		})
		require.NoError(t, err)
		require.NotEmpty(t, id)
		require.NotEmpty(t, auth)

		op, err := r.GetOperation(ctx, &longrunningv1.GetOperationRequest{
			UniqueId: id,
		})
		require.NoError(t, err)
		require.NotNil(t, op)
		require.NotNil(t, op.CreateTime)
		require.NotNil(t, op.LastUpdate)

		op.CreateTime = nil
		op.LastUpdate = nil

		expected := &longrunningv1.Operation{
			UniqueId:    id,
			Owner:       "test",
			Creator:     "test-case",
			State:       longrunningv1.OperationState_OperationState_RUNNING,
			Ttl:         durationpb.New(time.Minute),
			GracePeriod: durationpb.New(time.Second),
			Description: "A simple test operation",
			Parameters: map[string]*structpb.Value{
				"param1": param1,
			},
			Annotations: map[string]string{
				"foo": "bar",
			},
			Kind: "test-op",
		}

		if !proto.Equal(op, expected) {
			expectedBlob, _ := protojson.MarshalOptions{Indent: "  "}.Marshal(expected)
			actualBlob, _ := protojson.MarshalOptions{Indent: "  "}.Marshal(op)

			t.Errorf("got unexpected result:\n\texpected:\n%s\nactual:\n%s", string(expectedBlob), string(actualBlob))
		}
	})

	t.Run("UpdateOperation", func(t *testing.T) {
		op, err := r.UpdateOperation(ctx, &longrunningv1.UpdateOperationRequest{
			UniqueId:  id,
			Running:   false,
			AuthToken: auth,
			Annotations: map[string]string{
				"bar": "foo",
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"running",
				},
			},
		})
		require.NoError(t, err)

		require.Equal(t, longrunningv1.OperationState_OperationState_PENDING, op.State)
		require.Equal(t, map[string]string{"foo": "bar"}, op.Annotations) // should not have been updated

		op, err = r.UpdateOperation(ctx, &longrunningv1.UpdateOperationRequest{
			UniqueId:  id,
			AuthToken: auth,
			Running:   true,
			Annotations: map[string]string{
				"bar": "foo",
			},
		})
		require.NoError(t, err)

		require.Equal(t, longrunningv1.OperationState_OperationState_RUNNING, op.State)
		require.Equal(t, map[string]string{"bar": "foo"}, op.Annotations) // should not have been updated
	})

	t.Run("UpdateOperation_NoAuthToken", func(t *testing.T) {
		op, err := r.UpdateOperation(ctx, &longrunningv1.UpdateOperationRequest{
			UniqueId: id,
			Running:  false,
			Annotations: map[string]string{
				"bar": "foo",
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"running",
				},
			},
		})
		require.Error(t, err)
		require.Nil(t, op)
	})
}
