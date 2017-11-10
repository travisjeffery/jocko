FROM golang:1.9.1 as builder
LABEL builder=true
RUN go get -u github.com/golang/dep/cmd/dep
ADD . /go/src/github.com/travisjeffery/jocko
WORKDIR /go/src/github.com/travisjeffery/jocko
RUN make deps build-static

FROM scratch
#FROM busybox:latest
COPY --from=builder /go/src/github.com/travisjeffery/jocko/jocko /bin/jocko
EXPOSE 9092 9093 9094 9095
VOLUME "/tmp/jocko"
CMD ["jocko"]
