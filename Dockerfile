FROM golang:latest as build-base
ADD . /go/src/github.com/travisjeffery/jocko
WORKDIR /go/src/github.com/travisjeffery/jocko
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cmd/jocko/jocko cmd/jocko/*.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cmd/kadmin/kadmin cmd/kadmin/*.go

FROM alpine:latest
COPY --from=build-base /go/src/github.com/travisjeffery/jocko/cmd/kadmin/kadmin /usr/local/bin/kadmin
COPY --from=build-base /go/src/github.com/travisjeffery/jocko/cmd/jocko/jocko /usr/local/bin/jocko
EXPOSE 9092 9093 9094 9095
VOLUME "/tmp/jocko"
CMD ["jocko", "broker"]
