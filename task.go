package conduit

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	protos "github.com/hamersaw/conduit-poc/protos/gen/pb-go"
)

type Task struct {
	ID                    string
	Topic                 string
	ExecutionDurationMs   int
	CompletedTs           time.Time
	FinalizedTs           time.Time
	HeartbeatExpirationTs time.Time
	InitializedTs         time.Time
	LeaseExpirationTs     time.Time
	StartedTs             time.Time
}

func FromProto(task protos.Task) Task {
	return Task{
		ID:                  task.Id,
		Topic:               task.Topic,
		ExecutionDurationMs: int(task.ExecutionDurationMs),
		CompletedTs:         task.CompletedTs.AsTime(), 
		FinalizedTs:         task.FinalizedTs.AsTime(),
		InitializedTs:       task.InitializedTs.AsTime(),
		StartedTs:           task.StartedTs.AsTime(),
	}
}

func (t *Task) ToProto() *protos.Task {
	return &protos.Task{
		Id:                  t.ID,
		Topic:               t.Topic,
		ExecutionDurationMs: int32(t.ExecutionDurationMs),
		CompletedTs:         timestamppb.New(t.CompletedTs), 
		FinalizedTs:         timestamppb.New(t.FinalizedTs),
		InitializedTs:       timestamppb.New(t.InitializedTs),
		StartedTs:           timestamppb.New(t.StartedTs),
	}
}
