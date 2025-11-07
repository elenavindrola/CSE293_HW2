#!/bin/bash
# Build script for MapReduce Docker images

set -e

# Build worker image
docker build -f Dockerfile.worker -t mapreduce-worker:latest .


# Build coordinator image
docker build -f Dockerfile.coordinator -t mapreduce-coordinator:latest .

