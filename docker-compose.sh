#!/bin/bash

echo "removing old containers"
docker rm ipfs0
docker rm cluster0

docker-compose build
[ $? -eq 0 ] || exit $?;

echo "Start"
docker-compose up
[ $? -eq 0 ] || exit $?;

trap "docker-compose kill" INT