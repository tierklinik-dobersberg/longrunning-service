package repo

import (
	"context"
	"errors"
	"fmt"

	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var ErrNotFound = errors.New("operation not found")

type Repo struct {
	col *mongo.Collection
}

func NewRepo(ctx context.Context, url string, db string) (*Repo, error) {
	clientOptions := options.Client().ApplyURI(url)

	cli, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	r := &Repo{
		col: cli.Database(db).Collection("long-running-operations"),
	}

	return r, nil
}

func (r *Repo) find(ctx context.Context, filter bson.M) ([]*longrunningv1.Operation, error) {
	res, err := r.col.Find(ctx, filter)
	if err != nil {
		return nil, err
	}

	// FIXME
	var models []interface {
		ToProto() *longrunningv1.Operation
	}

	if err := res.All(ctx, &models); err != nil {
		return nil, fmt.Errorf("failed to decode office-hour documents: %w", err)
	}

	pbRes := make([]*longrunningv1.Operation, len(models))
	for idx, m := range models {
		pbRes[idx] = m.ToProto()
	}

	return pbRes, nil
}
