package org.example.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CliArgumentsTest {

    @Test
    void parsesRequiredFlags() {
        CliArguments arguments = CliArguments.parse(new String[]{
                "--brokers", "localhost:9092",
                "--topic", "person-topic",
                "--json", "sample.json"
        });

        assertEquals("localhost:9092", arguments.brokers());
        assertEquals("person-topic", arguments.topic());
        assertEquals("sample.json", arguments.jsonPath());
        assertTrue(!arguments.helpRequested());
    }

    @Test
    void supportsHelpFlag() {
        CliArguments arguments = CliArguments.parse(new String[]{"--help"});

        assertTrue(arguments.helpRequested());
    }

    @Test
    void throwsWhenRequiredFlagIsMissing() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> CliArguments.parse(new String[]{"--brokers", "localhost:9092"})
        );

        assertTrue(exception.getMessage().contains("--topic"));
    }
}
