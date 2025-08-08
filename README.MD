# Message Broker CLI Tool

## Overview
This project implements a CLI tool in Java that sends a [Protocol Buffers (protobuf)](https://developers.google.com/protocol-buffers) message to a Kafka message broker. The tool reads a JSON file, converts its content into a protobuf message, and publishes it to Kafka. The solution is fully tested using integration tests with [Testcontainers](https://www.testcontainers.org/) to ensure that messages written to the broker are readable by a consumer.

## Goals & Acceptance Criteria
- **Goal:** Implement a CLI tool to send protobuf messages to a message broker (Kafka).
- **Acceptance Criteria:**
  - The CLI reads a JSON file (provided as a parameter).
  - The JSON is converted to a protobuf `Person` message:
    ```protobuf
    message Person {
      required string name = 1;
      required int32 id = 2;
      optional string email = 3;
    }
    ```
  - The message is sent to Kafka.
  - The message can be read by a consumer.

## Solution Highlights
- **Language:** Java
- **Broker:** Kafka
- **Protobuf:** Used for message serialization
- **Tested with Testcontainers:** Integration tests spin up a real Kafka broker in Docker, send a message, and verify it is consumed.
- **Clean code:** Follows software engineering best practices and clean code principles.

## How to Run
I have a test where I demo Producer and consumer being used together, so you can run the integration test to see the full flow in action.

### Prerequisites
- Java 21+
- Docker (for running integration tests)
- Gradle (or use the provided `gradlew` wrapper)

### Build the Project
```sh
./gradlew build
```

### Example JSON Input
Place a file like this at `src/test/resources/sample_person.json`:
```json
{
  "name": "Alice",
  "id": 123,
  "email": "alice@example.com"
}
```

## Testing
Integration tests are provided using JUnit and Testcontainers. These tests:
- Start a Kafka broker in Docker
- Send a protobuf message using the CLI logic
- Consume the message and verify its content

To run the tests:
```sh
./gradlew test
```

## Documentation
- The CLI tool is documented in the code and this README.
- The integration test (`ProducerConsumerIntegrationTest`) demonstrates end-to-end message flow and validates the solution against the acceptance criteria.

---

**This project demonstrates a robust, testable, and maintainable approach to sending protobuf messages to Kafka, with full verification using Testcontainers.**

