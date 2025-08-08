package org.example.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.proto.PersonOuterClass.Person;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProducerConsumerIntegrationTest {

    private static final String TOPIC = "person-topic";
    private static final String SAMPLE_JSON = "src/test/resources/sample_person.json";

    private KafkaContainer kafkaContainer;
    private String bootstrapServers;
    private ProducerCli producerCli;
    private ConsumerCli consumerCli;

    @BeforeAll
    void setUp() {
        DockerImageName image = DockerImageName.parse("apache/kafka:3.9.1");
        kafkaContainer = new KafkaContainer(image);
        kafkaContainer.start();
        bootstrapServers = kafkaContainer.getBootstrapServers();

        producerCli = new ProducerCli();
        consumerCli = new ConsumerCli();
    }

    @AfterAll
    void tearDown() {
        kafkaContainer.stop();
    }

    @Test
    public void testProduceAndConsumePerson() throws Exception {
        // Produce
        Person person = producerCli.loadPersonFromJson(SAMPLE_JSON);
        producerCli.sendPersonToKafka(bootstrapServers, TOPIC, person);

        // Consume
        Properties consumerProps = consumerCli.createConsumerProperties(bootstrapServers);
        consumerProps.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            AtomicReference<Person> receivedPerson = new AtomicReference<>();
            long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
            while (receivedPerson.get() == null && System.currentTimeMillis() < end) {
                consumerCli.subscribeAndConsume(consumer, TOPIC, receivedPerson::set);
                Thread.sleep(200);
            }
            assertNotNull(receivedPerson.get(), "Person should be received from Kafka");
            assertEquals(person.getId(), receivedPerson.get().getId());
            assertEquals(person.getName(), receivedPerson.get().getName());
            assertEquals(person.getEmail(), receivedPerson.get().getEmail());
        }
    }
}
