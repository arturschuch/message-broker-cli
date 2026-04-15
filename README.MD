# Message Broker CLI

CLI tool in Java that reads a JSON payload, converts it to a Protocol Buffers `Person` message, and publishes it to Kafka.

## Features

- Sends protobuf payloads to Kafka from the command line
- Uses generated classes from `person.proto`
- Includes integration tests with Testcontainers

## Prerequisites

- Java 21+
- Docker (for integration tests)

## Build

```sh
./gradlew clean build
```

## Run as CLI

```sh
./gradlew run --args="--brokers localhost:9092 --topic person-topic --json src/test/resources/sample_person.json"
```

### Options

- `--brokers` Kafka bootstrap servers (required)
- `--topic` Kafka topic name (required)
- `--json` Path to JSON file (required)
- `--help` Show usage

## Example JSON

```json
{
  "name": "Alice",
  "id": 123,
  "email": "alice@example.com"
}
```

## Test

```sh
./gradlew test
```

The integration test starts Kafka in Docker, publishes a protobuf message, consumes it, and verifies the payload.
