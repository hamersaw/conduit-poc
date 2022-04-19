package conduit

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	//"github.com/uptrace/bun/extra/bundebug"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	proto "github.com/hamersaw/conduit-poc/protos/gen/pb-go"
)

type Task struct {
	bun.BaseModel                       `bun:"table:tasks"`
	ID                    string        `bun:"id,pk"`
	Topic                 string        `bun:"topic,notnull"`
	ExecutionDuration     time.Duration `bun:"execution_duration,notnull"`
	CompletedAt           *time.Time    `bun:"completed_at"`
	FinalizedAt           *time.Time    `bun:"finalized_at"`
	HeartbeatExpirationAt *time.Time    `bun:"heartbeat_expiration_at"`
	InitializedAt         *time.Time    `bun:"initialized_at"`
	LeaseExpirationAt     *time.Time    `bun:"lease_expiration_at"`
	StartedAt             *time.Time    `bun:"started_at"`
}

func FromProto(t proto.Task) *Task {
	task := Task {
		ID:                t.Id,
		Topic:             t.Topic,
		ExecutionDuration: t.ExecutionDuration.AsDuration(),
	}

	if t.CompletedAt != nil {
		ts := t.CompletedAt.AsTime()
		task.CompletedAt = &ts
	}
	if t.FinalizedAt != nil {
		ts := t.FinalizedAt.AsTime()
		task.FinalizedAt = &ts
	}
	if t.HeartbeatExpirationAt != nil {
		ts := t.HeartbeatExpirationAt.AsTime()
		task.HeartbeatExpirationAt = &ts
	}
	if t.InitializedAt != nil {
		ts := t.InitializedAt.AsTime()
		task.InitializedAt = &ts
	}
	if t.LeaseExpirationAt != nil {
		ts := t.LeaseExpirationAt.AsTime()
		task.LeaseExpirationAt = &ts
	}
	if t.StartedAt != nil {
		ts := t.StartedAt.AsTime()
		task.StartedAt = &ts
	}

	return &task
}

func (t *Task) ToProto() *proto.Task {
	task := &proto.Task{
		Id:                    t.ID,
		Topic:                 t.Topic,
		ExecutionDuration:     durationpb.New(t.ExecutionDuration),
	}

	if t.CompletedAt != nil {
		task.CompletedAt = timestamppb.New(*t.CompletedAt)
	}
	if t.FinalizedAt != nil {
		task.FinalizedAt = timestamppb.New(*t.FinalizedAt)
	}
	if t.HeartbeatExpirationAt != nil {
		task.HeartbeatExpirationAt = timestamppb.New(*t.HeartbeatExpirationAt)
	}
	if t.InitializedAt != nil {
		task.InitializedAt = timestamppb.New(*t.InitializedAt)
	}
	if t.LeaseExpirationAt != nil {
		task.LeaseExpirationAt = timestamppb.New(*t.LeaseExpirationAt)
	}
	if t.StartedAt != nil {
		task.StartedAt = timestamppb.New(*t.StartedAt)
	}

	return task
}

func OpenDB(ctx context.Context, host string, port int, username, password, database string) (*bun.DB, error) {
	pgConnection := pgdriver.NewConnector(
		pgdriver.WithNetwork("tcp"),
		pgdriver.WithAddr(fmt.Sprintf("%s:%d", host, port)),
		pgdriver.WithInsecure(true),
		pgdriver.WithUser(username),
		pgdriver.WithPassword(password),
		pgdriver.WithDatabase(database))

	sqlDB := sql.OpenDB(pgConnection)
	db := bun.NewDB(sqlDB, pgdialect.New())
	//db.AddQueryHook(bundebug.NewQueryHook()) // print failed queries
	//db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true))) // print all queries

	// TODO - capture table already exists
	db.NewCreateTable().Model((*Task)(nil)).Exec(ctx)
	return db, nil
}

func CreateTask(ctx context.Context, db *bun.DB, task *Task) error {
	_, err := db.NewInsert().Model(task).Exec(ctx)
	return err
}

func GetBufferTasks(ctx context.Context, db *bun.DB, topic string, limit int) ([]*Task, error) {
	now := time.Now()

	// begin a new transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	// query
	var tasks []*Task
	err = tx.NewSelect().
		Model(&tasks).
		Where("topic = ?", topic).
		Where("lease_expiration_at IS NULL").WhereOr("lease_expiration_at < ?", now).
		Where("heartbeat_expiration_at IS NULL").WhereOr("heartbeat_expiration_at < ?", now).
		Limit(limit).
		For("UPDATE").
		Scan(ctx)

	if err != nil {
		tx.Rollback()
		return nil, err
	}

	// update lease on results 
	leaseExpirationAt := now.Add(time.Second * 40) // TODO - parameterize
	for _, task := range tasks {
		task.LeaseExpirationAt = &leaseExpirationAt
	}

	if len(tasks) > 0 {
		_, err = tx.NewUpdate().
			Model(&tasks).
			Column("lease_expiration_at").
			Bulk().
			Exec(ctx)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	// commit and complete
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, err
	}

	return tasks, nil
}
