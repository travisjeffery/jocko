BUILD_PATH := jocko
DOCKER_TAG := latest

all: deps test

deps:
	@which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
	@dep ensure

build:
	@go build -o $(BUILD_PATH) cmd/jocko/main.go

release:
	@which goreleaser 2>/dev/null || go get -u github.com/goreleaser/goreleaser
	@goreleaser

clean:
	@rm -rf dist

build-docker:
	@docker build -t travisjeffery/jocko:$(DOCKER_TAG) .

test:
	@go test -v ./...

test-race:
	@go test -v -race -p=1 ./...

.PHONY: test-race test build-docker clean release build deps all
