# Jocko cluster example

This will start a local three node cluster.

## Build

```bash
$ go get github.com/travisjeffery/jocko
$ cd $GOPATH/src/github.com/travisjeffery/jocko/cmd/jocko
$ go build
```

## Start the nodes

```bash
$ ./jocko --debug \
          --logdir="/tmp/jocko1" \
          --tcpaddr=127.0.0.1:9001 \
          --raftdir="/tmp/jocko1/raft" \
          --raftaddr=127.0.0.1 \
          --raftport=8001 \
          --serfport=7946 \
          --id=1
          
$ ./jocko --debug \
          --logdir="/tmp/jocko2" \
          --tcpaddr=127.0.0.1:9002 \
          --raftdir="/tmp/jocko2/raft" \
          --raftaddr=127.0.0.1 \
          --raftport=8002 \
          --serfport=7947 \
          --serfmembers=127.0.0.1:7946 \
          --id=2
          
$ ./jocko --debug \
          --logdir="/tmp/jocko3" \
          --tcpaddr=127.0.0.1:9003 \
          --raftdir="/tmp/jocko3/raft" \
          --raftaddr=127.0.0.1 \
          --raftport=8003 \
          --serfport=7948 \
          --serfmembers=127.0.0.1:7946 \
          --id=3
```
