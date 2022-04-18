package conduit

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// TODO - should use prepared queries for all of these - https://pkg.go.dev/database/sql#Tx.Prepare
const (
	insertStmt = `insert into tasks(id, topic, execution_duration, completed_ts, finalized_ts, heartbeat_expiration_ts, initialized_ts, lease_expiration_ts, started_ts) values($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	selectBufferStmt = `select id, topic, execution_duration, completed_ts, finalized_ts, heartbeat_expiration_ts, initialized_ts, lease_expiration_ts, started_ts from tasks where topic=$1 and (lease_expiration_ts is null or lease_expiration_ts<$2) and (heartbeat_expiration_ts is null or heartbeat_expiration_ts<$3) limit $4 for update`
	//updateCompletedStmt = `update "tasks" set "lease_expiration_ms"=$1 where "id"=$2`
	//updateHeartbeattmt = `update "tasks" set "heartbeat_expiration_ms"=$1 where "id"=$2`
	updateLeaseStmt = `update "tasks" set "lease_expiration_ts"=$1 where "id"=$2`
	//updateStartedStmt = `update "tasks" set "started_ts"=$1 where "id"=$2`
)

func OpenDB(host string, port int, username, password, database string) (*sql.DB, error) {
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, username, password, database)

	return sql.Open("postgres", connectionString)
}

func CreateTask(db *sql.DB, task *Task) error {
	_, err := db.Exec(insertStmt, task.ID, task.Topic, task.ExecutionDuration, task.CompletedTs, task.FinalizedTs,
		task.HeartbeatExpirationTs, task.InitializedTs, task.LeaseExpirationTs, task.StartedTs)
	return err
}

func GetBufferTasks(ctx context.Context, db *sql.DB, topic string, limit int) ([]*Task, error) {
	// start a new transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	// query 
	log.Printf("querying for %d task(s)", limit)
	rows, err := tx.Query(selectBufferStmt, topic, time.Now(), time.Now(), limit)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	tasks := make([]*Task, 0)
	for rows.Next() {
		task := Task{}
		err := rows.Scan(&task.ID, &task.Topic, &task.ExecutionDuration, &task.CompletedTs, &task.FinalizedTs,
			&task.HeartbeatExpirationTs, &task.InitializedTs, &task.LeaseExpirationTs, &task.StartedTs)

		if err != nil {
			tx.Rollback()
			return nil, err
		}

		tasks = append(tasks, &task)
	}

	log.Printf("parsed %d task(s)", len(tasks))

	// update the results
	leaseExpirationTs := time.Now().Add(time.Second * 40)
	for _, task := range tasks {
		if _, err := tx.Exec(updateLeaseStmt, leaseExpirationTs, task.ID); err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return nil, err
	}

	return tasks, nil
}
