package org.flossware.messaging;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MessageClient implementation for Apache Kafka.
 *
 * <p>Supports reading and writing messages to Kafka topics. Messages are keyed by string
 * and stored as byte arrays. Maintains an internal cache of consumed messages.</p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * MessageClient kafka = KafkaMessageClient.builder()
 *     .bootstrapServers("localhost:9092")
 *     .topic("events")
 *     .groupId("my-consumer-group")
 *     .build();
 *
 * // Write message
 * kafka.write("user-123", "event data".getBytes());
 *
 * // Read message (polls from topic)
 * byte[] data = kafka.read("user-123");
 *
 * kafka.close();
 * }</pre>
 */
public class KafkaMessageClient implements MessageClient {
    private final KafkaProducer<String, byte[]> producer;
    private final KafkaConsumer<String, byte[]> consumer;
    private final String topic;
    private final Map<String, byte[]> messageCache;
    private final long pollTimeoutMs;

    private KafkaMessageClient(String bootstrapServers, String topic, String groupId, long pollTimeoutMs) {
        this.topic = Objects.requireNonNull(topic, "topic cannot be null");
        this.messageCache = new ConcurrentHashMap<>();
        this.pollTimeoutMs = pollTimeoutMs;

        // Producer configuration
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);

        // Consumer configuration
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId != null ? groupId : UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProps);

        // Subscribe to topic
        consumer.subscribe(Collections.singletonList(topic));

        // Load existing messages
        pollMessages();
    }

    private void pollMessages() {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
        for (ConsumerRecord<String, byte[]> record : records) {
            if (record.key() != null && record.value() != null) {
                messageCache.put(record.key(), record.value());
            }
        }
    }

    @Override
    public void write(String key, byte[] value) throws IOException {
        try {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
            producer.send(record).get();
            messageCache.put(key, value);
        } catch (Exception e) {
            throw new IOException("Failed to write message to Kafka: " + key, e);
        }
    }

    @Override
    public byte[] read(String key) throws IOException {
        if (messageCache.containsKey(key)) {
            return messageCache.get(key);
        }

        // Poll for new messages
        pollMessages();

        return messageCache.get(key);
    }

    @Override
    public boolean exists(String key) throws IOException {
        if (messageCache.containsKey(key)) {
            return true;
        }
        pollMessages();
        return messageCache.containsKey(key);
    }

    @Override
    public List<String> listKeys(String prefix) throws IOException {
        throw new UnsupportedOperationException("Kafka does not support key listing");
    }

    @Override
    public void delete(String key) throws IOException {
        // Kafka doesn't support deletion by key - write tombstone (null value)
        write(key, null);
        messageCache.remove(key);
    }

    @Override
    public String getDescription() {
        return "Kafka[topic=" + topic + ", cached=" + messageCache.size() + "]";
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String bootstrapServers;
        private String topic;
        private String groupId;
        private long pollTimeoutMs = 1000;

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder pollTimeout(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return this;
        }

        public KafkaMessageClient build() {
            Objects.requireNonNull(bootstrapServers, "bootstrapServers must be set");
            Objects.requireNonNull(topic, "topic must be set");
            return new KafkaMessageClient(bootstrapServers, topic, groupId, pollTimeoutMs);
        }
    }
}
