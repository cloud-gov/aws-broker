#!/bin/sh

set -e -x

cp secrets-test.yml secrets.yml
cp catalog-test.yml catalog.yml

# Run all _test.go files in main directory and subdirectories
go test ./...
