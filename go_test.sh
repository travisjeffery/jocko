#!/usr/bin/env bash

set -e

for d in $(go list ./... | grep -v 'golang/dep'); do
    go test -v "$d"
done
