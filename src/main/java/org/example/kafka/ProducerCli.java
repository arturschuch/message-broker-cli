package org.example.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.proto.PersonOuterClass.Person;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerCli {
    private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

    public Person loadPersonFromJson(String jsonPath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(new File(jsonPath));
        Person.Builder builder = Person.newBuilder()
                .setName(root.path("name").asText())
                .setId(root.path("id").asInt());
        if (root.has("email")) {
            builder.setEmail(root.get("email").asText());
        }
        return builder.build();
    }

    public void sendPersonToKafka(String brokers, String topic, Person person)
            throws ExecutionException, InterruptedException {
        Properties props = createProducerProperties(brokers);
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, byte[]> record =
                    new ProducerRecord<>(topic, String.valueOf(person.getId()), person.toByteArray());
            RecordMetadata meta = producer.send(record).get();
            log.info("Sent Person{{}} to {}[{}]@{}", person, meta.topic(), meta.partition(), meta.offset());
        }
    }

    private Properties createProducerProperties(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);
        return props;
    }
}