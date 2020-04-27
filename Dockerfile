# The file using for build smallest conatiner for production
# And there are additionals: `go test` step and non-root user unlike the Dockerfile-dev.

FROM golang:1.14-alpine as build-base
RUN install -g nobody -o nobody -m 0770 -d /tmp/empty-dir-owned-by-nobody
RUN echo 'nobody:x:65534:65534:nobody:/:/sbin/nologin' > /tmp/passwd

ADD . $GOPATH/src/github.com/travisjeffery/jocko/
WORKDIR $GOPATH/src/github.com/travisjeffery/jocko/
RUN go mod download && go mod verify
RUN CGO_ENABLED=0 go test -v ./...
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o /jocko cmd/jocko/main.go

FROM scratch
COPY --chown=65534:65534 --from=build-base /tmp/empty-dir-owned-by-nobody /tmp
COPY --from=build-base /jocko      /jocko
COPY --from=build-base /tmp/passwd /etc/passwd
EXPOSE 9092 9093 9094 9095
USER nobody
ENTRYPOINT ["/jocko"]
