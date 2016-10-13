# JOCKO

Kafka/distributed commit log service in Go.

## Goals of this project:

- Implement Kafka in Go
- Make operating simpler
- Distribute a single binary
- Improve performance
- Use Raft for consensus
- Smarter configuration settings, examples:
  - Topics A, B, C, D use 25% of available disk space each (right now you have to specify in byte size or age)
  - Handling size configs when you change the number of partitions or add more topics
- Learn a lot and have fun

## TODO

- [x] Write and read segments of a commit log
- [x] Segment indexes
- [ ] Writing over a network
- [ ] Distributed replication
- [ ] Clients
- [ ] Etc...

## License

MIT

--- 

- [travisjeffery.com](http://travisjeffery.com)
- GitHub [@travisjeffery](https://github.com/travisjeffery)
- Twitter [@travisjeffery](https://twitter.com/travisjeffery)
- Medium [@travisjeffery](https://medium.com/@travisjeffery)


