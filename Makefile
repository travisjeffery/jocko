BUILD_PATH := jocko
DOCKER_TAG := latest

all: deps test
.PHONY: all

.PHONY: deps
deps:
	@which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
	dep ensure

build:
	go build -o $(BUILD_PATH) cmd/jocko/main.go

build-static:
	CGO_ENABLED=0 go build -a --installsuffix cgo --ldflags="-s" -o $(BUILD_PATH) cmd/jocko/main.go

build-docker:
	docker build -t travisjeffery/jocko:$(DOCKER_TAG) .

.PHONY: test
test:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -v -race -p=1 ./...


