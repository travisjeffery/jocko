
all: deps test
.PHONY: all

.PHONY: deps
deps:
	@which dep 2>/dev/null || go get -u github.com/golang/dep/cmd/dep
	dep ensure

.PHONY: test
test:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -v -race -p=1 ./...


