BUILD_PATH := cmd/jocko/jocko
DOCKER_TAG := latest

all: test

deps:
	@which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
	@dep ensure -v

vet:
	@go list ./... | grep -v vendor | xargs go vet

build: deps
	@go build -o $(BUILD_PATH) cmd/jocko/*.go

build-kadmin: 
	@go build -o cmd/kadmin/kadmin cmd/kadmin/*.go

build-static:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o $(BUILD_PATH) cmd/jocko/*.go

build-static-kadmin:
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cmd/kadmin/kadmin cmd/kadmin/*.go

release:
	@which goreleaser 2>/dev/null || go get -u github.com/goreleaser/goreleaser
	@goreleaser

clean:
	@rm -rf dist

build-docker:
	@docker build -t travisjeffery/jocko:$(DOCKER_TAG) .

generate:
	@go generate

test: build
	@go test -v ./...

test-race:
	@go test -v -race -p=1 ./...

.PHONY: test-race test build-docker clean release build deps vet all
