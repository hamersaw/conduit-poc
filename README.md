# conduit-poc
## overview
proof-of-concept for task scheduling with distributed durable queues.

## usage
### postgres
    # start postgres instance
    docker run --name postgres --publish 5432:5432 --env POSTGRES_PASSWORD=foo -d postgres

    # connect to postgres using container psql
    docker exec -it postgres psql -h localhost -U postgres

    # create database
    CREATE DATABASE conduit;
    \connect conduit

## todo
- set StartedAt timestamp
- hook into prometheus metrics
    - queue count / size / throughput
    - postgres read & write ops/s / db size
- should we update lease and heartbeat by (existing + x) or just (now + x)

## references
https://shekhargulati.com/2022/01/27/correctly-using-postgres-as-queue/
[Devious SQL](https://blog.crunchydata.com/blog/message-queuing-using-native-postgresql)
[psql queue throughput](https://www.pgcon.org/2016/schedule/attachments/414_queues-pgcon-2016.pdf)

[Cherami](https://eng.uber.com/cherami-message-queue-system/)
[temporal architecture](https://www.youtube.com/watch?v=t524U9CixZ0)

https://martinfowler.com/articles/cd4ml.html
https://papers.nips.cc/paper/2015/file/86df7dcfd896fcaf2674f757a2463eba-Paper.pdf

service/matching/matchingEngine.go:288
    when a task get scheduled - send first to long polling client
service/matching/db.go:95
    task queues hold lease over persistent layer
service/matching/taskReader.go:50
    store in-memory buffered task queues
    buffer writes / updates back (how to guarantee persistence then?)
        retry on failure?
service/matching/taskWriter.go
    writeTaskLoop that reads <requests, reponse chan> from chan
    ensures only one entity is writing or updating at a time
