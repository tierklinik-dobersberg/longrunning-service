package repo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrNotFound = errors.New("operation not found")

type Repo struct {
	col *mongo.Collection
	cli *mongo.Client
}

func NewRepo(ctx context.Context, url string, db string) (*Repo, error) {
	clientOptions := options.Client().ApplyURI(url)

	cli, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	return NewRepoWithClient(ctx, cli, db)
}

func NewRepoWithClient(ctx context.Context, cli *mongo.Client, db string) (*Repo, error) {
	r := &Repo{
		col: cli.Database(db).Collection("long-running-operations"),
		cli: cli,
	}

	return r, nil
}

func (r *Repo) RegisterOperation(ctx context.Context, reg *longrunningv1.RegisterOperationRequest) (string, string, error) {
	var authCodeBytes [32]byte
	if _, err := rand.Read(authCodeBytes[:]); err != nil {
		return "", "", err
	}

	authCode := hex.EncodeToString(authCodeBytes[:])

	model, err := operationFromRegistrationRequest(reg)
	if err != nil {
		return "", "", err
	}

	model.ID = primitive.NewObjectID()
	model.AuthToken = authCode

	if model.State == longrunningv1.OperationState_OperationState_UNSPECIFIED {
		model.State = longrunningv1.OperationState_OperationState_PENDING
	}

	if _, err := r.col.InsertOne(ctx, model); err != nil {
		return "", "", err
	}

	return model.ID.Hex(), authCode, nil
}

func (r *Repo) GetActiveOperations(ctx context.Context) ([]*longrunningv1.Operation, error) {
	return r.find(ctx, bson.M{
		"state": longrunningv1.OperationState_OperationState_RUNNING,
	})
}

func (r *Repo) MarkAsLost(ctx context.Context, id string) (*longrunningv1.Operation, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	updDoc := bson.M{
		"lastUpdate": time.Now(),
		"state":      longrunningv1.OperationState_OperationState_LOST,
	}

	return run(ctx, r, func(sc mongo.SessionContext) (*longrunningv1.Operation, error) {
		result, err := r.findAndUpdateOperation(ctx, oid, updDoc)
		if err != nil {
			return nil, err
		}

		return result.ToProto()
	})
}

func (r *Repo) CompleteOperation(ctx context.Context, upd *longrunningv1.CompleteOperationRequest) (*longrunningv1.Operation, error) {
	id, err := primitive.ObjectIDFromHex(upd.UniqueId)
	if err != nil {
		return nil, err
	}

	updDoc := bson.M{
		"lastUpdate": time.Now(),
		"state":      longrunningv1.OperationState_OperationState_COMPLETE,
	}

	switch v := upd.Result.(type) {
	case *longrunningv1.CompleteOperationRequest_Error:
		updDoc["error"] = Error{
			Message: v.Error.Message,
			Details: v.Error.ErrorDetails,
		}

	case *longrunningv1.CompleteOperationRequest_Success:
		updDoc["success"] = Success{
			Message: v.Success.Message,
			Result:  v.Success.Result,
		}

	default:
		return nil, fmt.Errorf("missing result value")
	}

	return run(ctx, r, func(ctx mongo.SessionContext) (*longrunningv1.Operation, error) {
		// first, query the operation to validate the update request.
		if _, err := r.getAndValidateUpdate(ctx, id, upd.AuthToken); err != nil {
			return nil, err
		}

		op, err := r.findAndUpdateOperation(ctx, id, updDoc)
		if err != nil {
			return nil, err
		}

		return op.ToProto()
	})
}

func (r *Repo) GetOperation(ctx context.Context, req *longrunningv1.GetOperationRequest) (*longrunningv1.Operation, error) {
	id, err := primitive.ObjectIDFromHex(req.UniqueId)
	if err != nil {
		return nil, err
	}

	res := r.col.FindOne(ctx, bson.M{"_id": id})
	if err := res.Err(); err != nil {
		return nil, err
	}

	var op Operation
	if err := res.Decode(&op); err != nil {
		return nil, fmt.Errorf("failed to decode operation: %w", err)
	}

	return op.ToProto()
}

func (r *Repo) QueryOperations(ctx context.Context, query *longrunningv1.QueryOperationsRequest) ([]*longrunningv1.Operation, error) {
	filter := bson.M{}

	if c := query.Creator; c != "" {
		filter["creator"] = c
	}

	if o := query.Owner; o != "" {
		filter["owner"] = o
	}

	if s := query.State; s != longrunningv1.OperationState_OperationState_UNSPECIFIED {
		filter["state"] = s
	}

	if k := query.Kind; k != "" {
		filter["kind"] = k
	}

	return r.find(ctx, filter)
}

func (r *Repo) UpdateOperation(ctx context.Context, upd *longrunningv1.UpdateOperationRequest) (*longrunningv1.Operation, error) {
	id, err := primitive.ObjectIDFromHex(upd.UniqueId)
	if err != nil {
		return nil, err
	}

	updDoc := bson.M{
		"lastUpdate": time.Now(),
	}

	paths := []string{"running", "annotations"}
	if um := upd.GetUpdateMask().GetPaths(); len(um) > 0 {
		paths = um
	}

	for _, p := range paths {
		switch p {
		case "running":
			var s longrunningv1.OperationState
			if upd.Running {
				s = longrunningv1.OperationState_OperationState_RUNNING
			} else {
				s = longrunningv1.OperationState_OperationState_PENDING
			}

			updDoc["state"] = s

		case "annotations":
			updDoc["annotations"] = upd.Annotations

		default:
			return nil, fmt.Errorf("invalid field in update mask")
		}
	}

	return run(ctx, r, func(ctx mongo.SessionContext) (*longrunningv1.Operation, error) {
		// first, query the operation to validate the update request.
		if _, err := r.getAndValidateUpdate(ctx, id, upd.AuthToken); err != nil {
			return nil, err
		}

		// Perform the actual update.
		result, err := r.findAndUpdateOperation(ctx, id, updDoc)
		if err != nil {
			return nil, err
		}

		return result.ToProto()
	})
}

func (r *Repo) find(ctx context.Context, filter bson.M) ([]*longrunningv1.Operation, error) {
	res, err := r.col.Find(ctx, filter, options.Find().SetSort(bson.D{
		{
			Key:   "createTime",
			Value: -1,
		},
	}))
	if err != nil {
		return nil, err
	}

	// decode the operation models
	var models []Operation
	if err := res.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("failed to decode office-hour documents: %w", err)
	}

	// convert our local models to their protobuf value
	errs := new(multierror.Error)
	pbRes := make([]*longrunningv1.Operation, 0, len(models))
	for _, m := range models {
		pb, err := m.ToProto()
		if err != nil {
			errs.Errors = append(errs.Errors, fmt.Errorf("failed to convert operation with id %q: %w", m.ID.Hex(), err))
		} else {
			pbRes = append(pbRes, pb)
		}
	}

	return pbRes, errs.ErrorOrNil()
}

func (r *Repo) findOperation(ctx context.Context, id primitive.ObjectID) (*Operation, error) {
	bsonDoc := r.col.FindOne(ctx, bson.M{"_id": id})
	if err := bsonDoc.Err(); err != nil {
		return nil, err
	}

	var op Operation
	if err := bsonDoc.Decode(&op); err != nil {
		return nil, fmt.Errorf("failed to decode operation: %w", err)
	}

	return &op, nil
}

func (r *Repo) getAndValidateUpdate(ctx context.Context, id primitive.ObjectID, authToken string) (*Operation, error) {
	// first, query the operation to validate the update request.
	op, err := r.findOperation(ctx, id)
	if err != nil {
		return nil, err
	}

	// Validate that we are allowed to update the operation.
	if err := op.CanUpdate(authToken); err != nil {
		return nil, err
	}

	return op, nil
}

func (r *Repo) findAndUpdateOperation(ctx context.Context, id primitive.ObjectID, updDoc any) (*Operation, error) {
	res := r.col.FindOneAndUpdate(
		ctx,
		bson.M{"_id": id},
		bson.M{"$set": updDoc},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	)

	if err := res.Err(); err != nil {
		return nil, err
	}

	var op Operation
	if err := res.Decode(&op); err != nil {
		return nil, fmt.Errorf("failed to decode operation: %w", err)
	}

	return &op, nil
}

func run[T any](ctx context.Context, r *Repo, fn func(mongo.SessionContext) (T, error)) (T, error) {
	var empty T

	session, err := r.cli.StartSession()
	if err != nil {
		return empty, fmt.Errorf("failed to start session: %w", err)
	}
	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		res, err := fn(ctx)
		if err != nil {
			return empty, err
		}

		return res, nil
	})
	if err != nil {
		return empty, err
	}

	return result.(T), nil
}
