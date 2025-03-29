package repo

import (
	"errors"
	"fmt"
	"time"

	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Operation struct {
	// ID holds the ID of the operation.
	ID primitive.ObjectID `bson:"_id"`

	// CreateTime holds the time the operation has been created.
	CreateTime time.Time `bson:"createTime"`

	// LastUpdate holds the time at which the operation was last updated (i.e. pinged)
	LastUpdate time.Time `bson:"lastUpdate"`

	// Owner is the owner of the operation.
	Owner string `bson:"owner"`

	// Creator is the create of the operation, if any.
	Creator string `bson:"creator"`

	// State holds the current state of the operation.
	State longrunningv1.OperationState `bson:"state"`

	// Ttl holds the TTL at which the operation must be pinged.
	Ttl time.Duration `bson:"ttl"`

	// GracePeriod holds the grace-priod when a ping is missed before the operation
	// is considered lost.
	GracePeriod time.Duration `bson:"gracePeriod"`

	// Description holds an optional description of the operation.
	Description string `bson:"description"`

	// Parameters holds additional parameters that were used to create the operation.
	Parameters map[string]any `bson:"parameters"`

	// Annotations holds service specific annotations for this operation.
	Annotations map[string]string `bson:"annotations"`

	// Kind is a service specifc operation kind. It's value is opaque to the service but
	// should follow some rules to ensure uniqueness like using
	// 	- tkd.customer.v1/import-job
	//  - tkd.notify.v1/send-mails
	//  - tkd.backup.v1/creating-backup
	//
	Kind string `bson:"kind"`

	Success *Success `bson:"success,omitempty"`
	Error   *Error   `bson:"error,omitempty"`

	AuthToken string `bson:"authToken"`

	PercentDone   int    `bson:"percentDone"`
	StatusMessage string `bson:"statusMessage"`
}

type Success struct {
	Message string     `´bson:"message"`
	Result  *anypb.Any `bson:"result"`
}

type Error struct {
	Message string     `bson:"message"`
	Details *anypb.Any `bson:"details"`
}

func (op *Operation) ToProto() (*longrunningv1.Operation, error) {
	pbop := &longrunningv1.Operation{
		UniqueId:      op.ID.Hex(),
		CreateTime:    timestamppb.New(op.CreateTime),
		Owner:         op.Owner,
		Creator:       op.Creator,
		State:         op.State,
		Ttl:           durationpb.New(op.Ttl),
		GracePeriod:   durationpb.New(op.GracePeriod),
		Description:   op.Description,
		LastUpdate:    timestamppb.New(op.LastUpdate),
		Annotations:   op.Annotations,
		Kind:          op.Kind,
		StatusMessage: op.StatusMessage,
		PercentDone:   int32(op.PercentDone),
	}

	if len(op.Parameters) > 0 {
		pbop.Parameters = make(map[string]*structpb.Value)

		for key, val := range op.Parameters {
			pb, err := structpb.NewValue(val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert parameter value to structpb.Value: key=%q, error=%w", key, err)
			}

			pbop.Parameters[key] = pb
		}
	}

	switch {
	case op.Success != nil && op.Error != nil:
		return nil, fmt.Errorf("operation has success and error value")

	case op.Success != nil:
		pbop.Result = &longrunningv1.Operation_Success{
			Success: &longrunningv1.OperationSuccess{
				Message: op.Success.Message,
				Result:  op.Success.Result,
			},
		}

	case op.Error != nil:
		pbop.Result = &longrunningv1.Operation_Error{
			Error: &longrunningv1.OperationError{
				Message:      op.Error.Message,
				ErrorDetails: op.Error.Details,
			},
		}
	}

	return pbop, nil
}

func operationFromRegistrationRequest(op *longrunningv1.RegisterOperationRequest) (*Operation, error) {
	ttl := time.Minute * 5
	if op.Ttl.IsValid() {
		ttl = op.Ttl.AsDuration()
	}

	grace := time.Minute * 5
	if op.GracePeriod.IsValid() {
		grace = op.GracePeriod.AsDuration()
	}

	params := make(map[string]any, len(op.Parameters))
	for key, value := range op.Parameters {
		params[key] = value.AsInterface()
	}

	o := &Operation{
		Owner:       op.Owner,
		Creator:     op.Creator,
		Ttl:         ttl,
		GracePeriod: grace,
		Description: op.Description,
		Parameters:  params,
		Kind:        op.Kind,
		State:       op.InitialState,
		CreateTime:  time.Now(),
		LastUpdate:  time.Now(),
		Annotations: op.Annotations,
	}

	return o, nil
}

var (
	ErrInvalidAuthToken   = errors.New("invalid auth_token")
	ErrOperationCompleted = errors.New("operation already completed")
)

func (op Operation) CanUpdate(authToken string) error {
	if op.AuthToken != authToken {
		return ErrInvalidAuthToken
	}

	if op.State == longrunningv1.OperationState_OperationState_COMPLETE {
		return ErrOperationCompleted
	}

	return nil
}
