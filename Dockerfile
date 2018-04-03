FROM golang:1.9-alpine  
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make && \
    go get -u github.com/golang/dep/cmd/dep
ADD . /go/src/github.com/travisjeffery/jocko
WORKDIR /go/src/github.com/travisjeffery/jocko
RUN GOOS=linux GOARCH=amd64 make deps build

FROM alpine:latest
COPY --from=0 /go/src/github.com/travisjeffery/jocko/jocko /usr/local/bin/jocko
EXPOSE 9092 9093 9094 9095
VOLUME "/tmp/jocko"
CMD ["jocko"]
