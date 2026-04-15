package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.example.proto.PersonOuterClass.Person;

@Slf4j
public class MessageBrokerCli {
    public static void main(String[] args) {
        CliArguments cliArguments = CliArguments.parse(args);
        if (cliArguments.helpRequested()) {
            System.out.println(CliArguments.usage());
            return;
        }

        ProducerCli producerCli = new ProducerCli();
        try {
            Person person = producerCli.loadPersonFromJson(cliArguments.jsonPath());
            producerCli.sendPersonToKafka(cliArguments.brokers(), cliArguments.topic(), person);
            log.info("Message published successfully");
        } catch (Exception e) {
            log.error("Failed to publish message", e);
            System.err.println("ERROR: " + e.getMessage());
            System.err.println(CliArguments.usage());
            System.exit(1);
        }
    }
}
