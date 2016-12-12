# Contributing to Jocko

Thank you for contributing to Jocko!

Here's the [ROADMAP](https://github.com/travisjeffery/jocko/issues/1) where you'll find what could be worked on. The current goal is get feature parity with Kafka 0.10.x.

Here's the steps to contribute:

## Fork and clone the repo:

```
git clone git@github.com:your-username/jocko.git
```

## Get the deps:

```
govendor sync
```

- (If you don't have govendor, run: `go get -u github.com/kardianos/govendor`)


## Check the tests pass:

```
go test ./...
```

## Make your change. Write tests. Make the tests pass:

``` 
go test ./...
```

## [Write a good commit message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

## Push to your fork. Submit a pull request.

- Describe in bullet points what your change does and why.
- Link the equivalent code from the [Kafka repo](https://github.com/apache/kafka) or [docs](https://kafka.apache.org/documentation).

Thanks!
