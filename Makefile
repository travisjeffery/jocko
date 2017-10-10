
all: deps test
.PHONY: all

.PHONY: deps
deps:
	dep ensure

.PHONY: test
test:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -v -race -p=1 ./...


