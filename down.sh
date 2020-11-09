#!/bin/bash

set -eou pipefail

docker stop temporal-sql
docker rm temporal-sql
