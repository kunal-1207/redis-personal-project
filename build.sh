#!/bin/bash

# Build script for Redis personal project

echo "Building Redis server..."
go build -o redis-server cmd/main.go

if [ $? -eq 0 ]; then
    echo "Build successful! Run ./redis-server to start the server."
else
    echo "Build failed!"
fi