![Travis CI](https://travis-ci.org/travisjeffery/jocko.svg?branch=master)

# JOCKO

Kafka/distributed commit log service in Go.

## Goals of this project:

- Implement Kafka in Go
- Protocol compatible so Kafka clients and services work with Jocko
- Make operating simpler
- Distribute a single binary
- Use Raft for consensus
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
    - [ ] Consumer group
- [ ] Replication [current task]

## License

MIT

--- 

- [travisjeffery.com](http://travisjeffery.com)
- GitHub [@travisjeffery](https://github.com/travisjeffery)
- Twitter [@travisjeffery](https://twitter.com/travisjeffery)
- Medium [@travisjeffery](https://medium.com/@travisjeffery)


