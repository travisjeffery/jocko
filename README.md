# JOCKO

Kafka/distributed commit log service in Go.

[![Build Status](https://semaphoreci.com/api/v1/travisjeffery/jocko/branches/master/shields_badge.svg)](https://semaphoreci.com/travisjeffery/jocko) [![Join the chat at https://gitter.im/travisjeffery/jocko](https://badges.gitter.im/travisjeffery/jocko.svg)](https://gitter.im/travisjeffery/jocko?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![codecov](https://codecov.io/gh/travisjeffery/jocko/branch/master/graph/badge.svg)](https://codecov.io/gh/travisjeffery/jocko)

## Goals of this project:

- Implement Kafka in Go
- Protocol compatible with Kafka so Kafka clients and services work with Jocko
- Make operating simpler
- Distribute a single binary
- Use Serf for discovery, Raft for consensus (and remove the need to run ZooKeeper)
- Smarter configuration settings
    - Able to use percentages of disk space for retention policies rather than only bytes and time kept
    - Handling size configs when you change the number of partitions or add topics
- Learn a lot and have fun

## TODO

- [x] Producing
- [x] Fetching
- [x] Partition consensus and distribution
- [ ] Protocol
    - [x] Produce
    - [x] Fetch
    - [x] Metadata
    - [x] Create Topics
    - [x] Delete Topics
    - [ ] Consumer group [current task]
- [x] Discovery
- [ ] API versioning [more API versions to implement]
- [ ] Replication [first draft done - testing heavily now]

## Reading

- [How Jocko's built-in service discovery and consensus works](https://medium.com/the-hoard/building-a-kafka-that-doesnt-depend-on-zookeeper-2c4701b6e961#.uamxtq1yz)
- [How Jocko's (and Kafka's) storage internals work](https://medium.com/the-hoard/how-kafkas-storage-internals-work-3a29b02e026#.qfbssm978)

## Project Layout

```
├── broker        broker subsystem
├── cmd           commands
│   └── jocko     command to run a Jocko broker and manage topics
├── commitlog     low-level commit log implementation
├── examples      examples running/using Jocko
│   ├── cluster   example booting up a 3-broker Jocko cluster
│   └── sarama    example producing/consuming with Sarama
├── protocol      golang implementation of Kafka's protocol
├── prometheus    wrapper around Prometheus' client lib to handle metrics
├── server        API subsystem
└── testutil      test utils
    └── mock      mocks of the various subsystems
```

## Building

### Local

1. Clone Jocko

    ```
    $ go get github.com/travisjeffery/jocko
    ```

1. Build Jocko

    ```
    $ cd $GOPATH/src/github.com/travisjeffery/jocko
    $ make build
    ```

    (If you see an error about `dep` not being found, ensure that
    `$GOPATH/bin` is in your `PATH`)

### Docker

`docker build -t travisjeffery/jocko:latest .`

## License

MIT

---

- [travisjeffery.com](http://travisjeffery.com)
- GitHub [@travisjeffery](https://github.com/travisjeffery)
- Twitter [@travisjeffery](https://twitter.com/travisjeffery)

- Medium [@travisjeffery](https://medium.com/@travisjeffery)
