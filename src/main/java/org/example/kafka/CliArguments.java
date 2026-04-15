package org.example.kafka;

import java.util.HashMap;
import java.util.Map;

record CliArguments(String brokers, String topic, String jsonPath, boolean helpRequested) {
    private static final String BROKERS = "--brokers";
    private static final String TOPIC = "--topic";
    private static final String JSON = "--json";
    private static final String HELP = "--help";

    static CliArguments parse(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("No arguments provided.");
        }

        boolean helpRequested = false;
        Map<String, String> values = new HashMap<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (HELP.equals(arg)) {
                helpRequested = true;
                continue;
            }

            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + arg);
            }

            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("Missing value for argument: " + arg);
            }

            String value = args[++i].trim();
            if (value.isEmpty()) {
                throw new IllegalArgumentException("Empty value for argument: " + arg);
            }
            values.put(arg, value);
        }

        if (helpRequested) {
            return new CliArguments("", "", "", true);
        }

        String brokers = required(values, BROKERS);
        String topic = required(values, TOPIC);
        String jsonPath = required(values, JSON);
        return new CliArguments(brokers, topic, jsonPath, false);
    }

    static String usage() {
        return """
                Usage:
                  ./gradlew run --args="--brokers <host:port> --topic <topic> --json <path>"

                Example:
                  ./gradlew run --args="--brokers localhost:9092 --topic person-topic --json src/test/resources/sample_person.json"

                Options:
                  --brokers   Kafka bootstrap servers (required)
                  --topic     Kafka topic name (required)
                  --json      Path to JSON payload file (required)
                  --help      Show this help text
                """;
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required argument: " + key);
        }
        return value;
    }
}
