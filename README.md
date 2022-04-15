# conduit-poc
## overview
proof-of-concept for task scheduling with distributed durable queues.

# references
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

# notes
how do we scale so a queue is on two TaskServices?
    if we lease tasks from the queue (for our buffered queue)
        then a failure means that no other taskservice can pick up those tasks
        on restart (failure) the taskservice will receive a heartbeat meaning it was responsible for those tasks
            heartbeat reporting can failover to another task service
            meaning task hearbeat has a taskservice id (for which taskservice leased it)
        is there an extreme corner case where the taskservice is unable to receive heartbeats, but is not down?
    frontend initiates longpoll for work from one taskservice
        if it doesn't receive a task within n -> initiate longpoll from another
        in a busy system it should only contact a single taskservice (without work it may contact all)
        still get fast passthrough if taskservice createstask during longpoll
    report metrics on taskservice queue length so frontend doesn't have to choose randomly
        should improve speed significantly - otherwise if all are empty use a default (so long polls get work quick)
    **frontend, taskservice, and database can scale independently**
