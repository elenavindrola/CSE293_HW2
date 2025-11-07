#!/bin/bash

# Usage: ./run_docker.sh <num>
# Example: ./run_docker.sh 4

NUM=$1

docker compose -f docker-compose_${NUM}.yml up -d

docker logs -f coor &

LOG_PID=$!

read -p "Press ENTER to stop & exit, otherwise wait..." 

kill $LOG_PID 2>/dev/null
docker compose -f docker-compose_${NUM}.yml stop

rm -rf intermediate output
docker compose -f docker-compose_${NUM}.yml down
