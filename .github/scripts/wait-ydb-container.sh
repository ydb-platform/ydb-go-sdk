#!/bin/bash

while ! docker run --network host cr.yandex/yc/yandex-docker-local-ydb:latest /ydb -e grpc://localhost:2136 -d /local scheme ls; do
  echo wait db...
  sleep 3
done

echo DB available
