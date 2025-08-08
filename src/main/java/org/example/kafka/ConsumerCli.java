package org.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.proto.PersonOuterClass.Person;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerCli {

    private static final String GROUP_ID = "person-consumer-group";
    private static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    public Properties createConsumerProperties(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", GROUP_ID);
        props.put("key.deserializer", KEY_DESERIALIZER);
        props.put("value.deserializer", VALUE_DESERIALIZER);
        return props;
    }

    public void subscribeAndConsume(KafkaConsumer<String, byte[]> consumer, String topic, java.util.function.Consumer<Person> personHandler) {
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(10));
        for (ConsumerRecord<String, byte[]> rec : records) {
            try {
                Person p = Person.parseFrom(rec.value());
                personHandler.accept(p);
            } catch (Exception e) {
                log.error("Error while reading Person from Kafka", e);
            }
        }
    }

}