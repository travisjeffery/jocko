BUILD_PATH := cmd/jocko/jocko
DOCKER_TAG := latest

all: test

deps:
	@which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
	@dep ensure -v

vet:
	@go list ./... | grep -v vendor | xargs go vet

build: deps
	@go build -o $(BUILD_PATH) cmd/jocko/main.go

release:
	@which goreleaser 2>/dev/null || go get -u github.com/goreleaser/goreleaser
	@goreleaser

clean:
	@rm -rf dist

build-docker:
	@docker build -t travisjeffery/jocko:$(DOCKER_TAG) .

generate:
	@go generate

test:
	@echo "" > coverage.out
	for d in $(shell go list ./... | grep -v vendor); do \
                go test -v -race -coverprofile=profile.out -covermode=atomic $$d; \
		if [ -f profile.out ]; then \
                        cat profile.out >> coverage.txt; \
			rm profile.out; \
		fi; \
	done

test-race:
	@go test -v -race -p=1 ./...

.PHONY: test-race test build-docker clean release build deps vet all
