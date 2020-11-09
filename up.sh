#!/bin/bash

set -eou pipefail

docker run -d \
     --name temporal-sql \
     -p 31337:5432 \
     -e POSTGRES_DB=temporal-sql \
     -e POSTGRES_USER=baravelli \
     -e POSTGRES_PASSWORD=swordfish \
     postgres:latest
sleep 2
psql -f schema.sql postgresql://baravelli:swordfish@localhost:31337/temporal-sql
