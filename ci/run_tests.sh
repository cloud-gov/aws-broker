#!/bin/sh

set -e -x

cp secrets-test.yml secrets.yml
cp catalog-test.yml catalog.yml

go test
