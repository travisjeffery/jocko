# Jocko cluster example

This will start a local three node cluster.

## Build

```bash
$ go get github.com/travisjeffery/jocko/...
$ cd $GOPATH/src/github.com/travisjeffery/jocko/cmd/jocko
$ go build
```

## Start the nodes

```bash
$ ./jocko brokers \
          --debug \
          --logdir="/tmp/jocko1" \
          --brokeraddr=127.0.0.1:9001 \
          --raftaddr=127.0.0.1:9002 \
          --serfaddr=127.0.0.1:9003 \
          --id=1

$ ./jocko brokers \
          --debug \
          --logdir="/tmp/jocko2" \
          --brokeraddr=127.0.0.1:9101 \
          --raftaddr=127.0.0.1:9102 \
          --serfaddr=127.0.0.1:9103 \
          --serfmembers=127.0.0.1:9003 \
          --id=2

$ ./jocko brokers \
          --debug \
          --logdir="/tmp/jocko3" \
          --brokeraddr=127.0.0.1:9201 \
          --raftaddr=127.0.0.1:9202 \
          --serfaddr=127.0.0.1:9203 \
          --serfmembers=127.0.0.1:9003 \
          --id=3
```
