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
$ ./jocko broker \
          --data-dir="/tmp/jocko0" \
          --broker-addr=127.0.0.1:9001 \
          --raft-addr=127.0.0.1:9002 \
          --serf-addr=127.0.0.1:9003 \
          --http-addr=127.0.0.1:9004 \
          --id=1

$ ./jocko broker \
          --data-dir="/tmp/jocko1" \
          --broker-addr=127.0.0.1:9101 \
          --raft-addr=127.0.0.1:9102 \
          --serf-addr=127.0.0.1:9103 \
          --http-addr=127.0.0.1:9104 \
          --serf-members=127.0.0.1:9003 \
          --id=2

$ ./jocko broker \
          --data-dir="/tmp/jocko2" \
          --broker-addr=127.0.0.1:9201 \
          --raft-addr=127.0.0.1:9202 \
          --serf-addr=127.0.0.1:9203 \
          --http-addr=127.0.0.1:9204 \
          --serf-members=127.0.0.1:9003 \
          --id=3
```

## docker-compose cluster

To start a [docker compose](https://docs.docker.com/compose/) cluster use the provided `docker-compose.yml`.
