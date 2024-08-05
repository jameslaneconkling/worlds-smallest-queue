#!/bin/bash

set -e

PG_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')

docker run --rm --name pg-test -e POSTGRES_PASSWORD=test -d -p 127.0.0.1:$PG_PORT:5432 postgres:16

set +e
while true; do
    res=$(docker exec pg-test pg_isready)
    sleep 1
    if [[ "$res" == *"accepting connections" ]]; then
        break
    fi
done
set -e

PGPASSWORD=test docker exec pg-test createdb -U postgres worlds-smallest-queue-test

echo -e "POSTGRES PORT | \033[0;32m$PG_PORT\033[0m"
