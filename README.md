# Personal Redis Implementation

This is a personal implementation of a Redis-like server written in Go.

## Features

- TCP server listening on port 6379
- Supports basic Redis commands:
  - PING
  - ECHO
  - SET (with expiration options EX and PX)
  - GET
  - EXISTS
  - DEL
  - EXPIRE
  - INCR
- RESP (Redis Serialization Protocol) compliant
- In-memory key-value storage with expiration support

## Getting Started

### Prerequisites

- Go 1.22 or higher

### Running the Server

```bash
go run cmd/main.go
```

The server will start listening on port 6379.

### Building the Server

```bash
go build -o redis-server cmd/main.go
./redis-server
```

## Usage

Connect to the server using any Redis client:

```bash
redis-cli ping
redis-cli set mykey "Hello World"
redis-cli get mykey
```