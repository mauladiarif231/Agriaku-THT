#!/bin/bash

# Membangun image Docker
echo "Building Docker images..."
docker-compose build

# Menjalankan kontainer Docker
echo "Starting Docker containers..."
docker-compose up