#!/bin/bash
arc=$(arch)
repo=rockywei/perfectpostgres:5.6.$arc
docker build -t $repo --build-arg arch=$arc .
docker run -it -v $PWD:/home -w /home --network perfect-postgresql_default $repo /bin/bash -c \
"swift test"
