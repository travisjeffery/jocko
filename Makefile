
all: deps test
.PHONY: all

.PHONY: deps
deps:
	go get -u github.com/kardianos/govendor
	govendor sync 

.PHONY: test
test:
	govendor test -v -p=1 +local

.PHONY: test-race
test-race:
	govendor test -v -race -p=1 +local


