package repo

import (
	"fmt"
	"strings"
	"time"

	longrunningv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/longrunning/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/ql"
	"github.com/tierklinik-dobersberg/apis/pkg/ql/bsonql"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var Schema = ql.FieldList{
	ql.FieldSpec{
		Name:    "uniqueId",
		Aliases: []string{"id"},
		TypeResolver: ql.TypeResolverFunc(func(s string) (any, error) {
			oid, err := primitive.ObjectIDFromHex(s)
			return oid, err
		}),
		Description: "The unique ID of the operation",
		Data: map[string]any{
			bsonql.BSONFieldName: "_id",
		},
	},
	ql.FieldSpec{
		Name:         "createTime",
		Aliases:      []string{"createdAt"},
		TypeResolver: ql.TimeStartKeywordType(time.Local),
		Description:  "The time at which the operation has been created",
		Data: map[string]any{
			bsonql.BSONFieldName: "createTime",
		},
	},
	ql.FieldSpec{
		Name:         "lastUpdate",
		Aliases:      []string{"lastUpdatedAt", "updatedAt"},
		TypeResolver: ql.TimeStartKeywordType(time.Local),
		Description:  "The time at which the operation has been updated last",
		Data: map[string]any{
			bsonql.BSONFieldName: "lastUpdate",
		},
	},
	ql.FieldSpec{
		Name:        "owner",
		Description: "The owner of the operation",
		Data: map[string]any{
			bsonql.BSONFieldName: "owner",
		},
	},
	ql.FieldSpec{
		Name:        "creator",
		Description: "The creator of the operation",
		Data: map[string]any{
			bsonql.BSONFieldName: "creator",
		},
	},
	ql.FieldSpec{
		Name:        "description",
		Description: "The description of the operation",
		Data: map[string]any{
			bsonql.BSONFieldName: "description",
		},
	},
	ql.FieldSpec{
		Name:        "kind",
		Description: "The kind of the operation",
		Data: map[string]any{
			bsonql.BSONFieldName: "kind",
		},
	},
	ql.FieldSpec{
		Name:        "state",
		Description: "The state of the operation",
		TypeResolver: ql.TypeResolverFunc(func(s string) (any, error) {
			switch strings.ToLower(s) {
			case "unspecified":
				return longrunningv1.OperationState_OperationState_UNSPECIFIED, nil
			case "pending":
				return longrunningv1.OperationState_OperationState_PENDING, nil
			case "running":
				return longrunningv1.OperationState_OperationState_RUNNING, nil
			case "complete":
				return longrunningv1.OperationState_OperationState_COMPLETE, nil
			case "lost":
				return longrunningv1.OperationState_OperationState_LOST, nil

			default:
				return nil, fmt.Errorf("unsupported operation state %q", s)
			}
		}),
		Data: map[string]any{
			bsonql.BSONFieldName: "state",
		},
	},
}
