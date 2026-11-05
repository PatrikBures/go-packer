#!/usr/bin/env bash

find . -type f -name '*_test.go' -printf '%h\n' \
    | sort -u \
    | xargs go test -short
