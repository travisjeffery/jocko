FROM golang:1.9-alpine  

RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make

RUN go get -u github.com/kardianos/govendor 
ADD . /go/src/github.com/travisjeffery/jocko
WORKDIR /go/src/github.com/travisjeffery/jocko
RUN govendor sync
RUN govendor test -v -p=1 +local
RUN go build -o jocko cmd/jocko/main.go
RUN go build -o createtopic cmd/createtopic/main.go

FROM alpine:latest

COPY --from=0 /go/src/github.com/travisjeffery/jocko/jocko /usr/local/bin/jocko
COPY --from=0 /go/src/github.com/travisjeffery/jocko/createtopic /usr/local/bin/createtopic

EXPOSE 9092 9093 9094

VOLUME "/tmp/jocko"

CMD ["jocko"]
