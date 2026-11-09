# EventPulse

EventPulse is a Go-based service for publishing simulated tracking events through RabbitMQ. It currently provides the foundation of an event pipeline; connection-bound publisher recovery and its precise delivery guarantees are the focus of Phase 0.

## Features

- **RabbitMQ Connection Prototype**: Connection reconnection with exponential backoff; complete channel and topology recovery is planned in Phase 0.
- **Confirmed Publishing**: Persistent JSON messages published with RabbitMQ publisher confirms.
- **Declarative Topology**: Exchange, queue, and binding configuration in Go.
- **Traffic Simulation**: Configurable tracking-event generation.
- **Structured Logging**: Uses Go's standard `slog` for structured, leveled logging.
- **Environment Configuration**: Easy configuration via `.env` files.
- **Docker Integration**: Includes a `docker-compose.yml` for quick RabbitMQ setup.

## Prerequisites

- [Go](https://go.dev/doc/install) (version 1.25 or later)
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) (for running RabbitMQ)

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/4hbab/eventpulse.git
cd eventpulse
```

### 2. Configuration

Create a `.env` file in the root directory (or use environment variables):

```env
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
LOG_LEVEL=info
SERVER_PORT=8080
```

### 3. Start RabbitMQ

Use the provided Makefile to start RabbitMQ in a Docker container:

```bash
make rabbit
```

### 4. Run the application

```bash
make run
```

## Development

### Available Commands

- `make rabbit`: Start RabbitMQ container.
- `make rabbit-down`: Stop RabbitMQ container.
- `make run`: Run the application.
- `make test`: Run tests.
- `make tidy`: Tidy go modules, format code, and run vet.

## Engineering Guides

- [Phase 0: Build an Honest Reliability Foundation](guides/phase-0-reliability-foundation.md)

## Project Structure

- `cmd/eventpulse`: Main entry point of the application.
- `internal/broker`: RabbitMQ connection management and reconnection logic.
- `guides`: Implementation and reliability guides.
