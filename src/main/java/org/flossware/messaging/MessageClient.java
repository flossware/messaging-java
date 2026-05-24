package org.flossware.messaging;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Universal interface for messaging and cache operations across different systems.
 *
 * <p>Provides a unified API for reading and writing byte messages to Apache Kafka,
 * RabbitMQ, and Redis. All implementations are thread-safe for concurrent operations.</p>
 *
 * <h2>Supported Systems</h2>
 * <ul>
 *   <li>Apache Kafka - Distributed event streaming</li>
 *   <li>RabbitMQ - Message broker with AMQP</li>
 *   <li>Redis - In-memory data store with pub/sub</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * // Kafka
 * MessageClient kafka = KafkaMessageClient.builder()
 *     .bootstrapServers("localhost:9092")
 *     .topic("events")
 *     .build();
 *
 * kafka.write("user-123", "event data".getBytes());
 * byte[] data = kafka.read("user-123");
 * kafka.close();
 * }</pre>
 *
 * @see KafkaMessageClient
 * @see RabbitMqMessageClient
 * @see RedisMessageClient
 */
public interface MessageClient extends Closeable {

    /**
     * Writes a message with the given key.
     *
     * @param key The message key (topic key for Kafka, routing key for RabbitMQ, Redis key)
     * @param value The message content as bytes
     * @throws IOException If the write operation fails
     */
    void write(String key, byte[] value) throws IOException;

    /**
     * Reads a message by key.
     *
     * <p>Behavior varies by implementation:
     * <ul>
     *   <li>Kafka: Polls for messages with the given key</li>
     *   <li>RabbitMQ: Consumes message from queue</li>
     *   <li>Redis: GET operation for the key</li>
     * </ul>
     *
     * @param key The message key
     * @return The message content, or null if not found
     * @throws IOException If the read operation fails
     */
    byte[] read(String key) throws IOException;

    /**
     * Checks if a message/key exists.
     *
     * @param key The message key
     * @return true if exists, false otherwise
     * @throws IOException If the check fails
     */
    boolean exists(String key) throws IOException;

    /**
     * Lists all keys matching the given prefix.
     *
     * <p>Not supported by all implementations (e.g., Kafka doesn't support key listing).</p>
     *
     * @param prefix The key prefix to match
     * @return A list of matching keys
     * @throws IOException If the list operation fails
     * @throws UnsupportedOperationException If listing is not supported
     */
    List<String> listKeys(String prefix) throws IOException;

    /**
     * Deletes a message/key.
     *
     * @param key The message key to delete
     * @throws IOException If the delete operation fails
     */
    void delete(String key) throws IOException;

    /**
     * Gets a human-readable description of this message client.
     *
     * @return A description string (e.g., "Kafka[localhost:9092/events]")
     */
    String getDescription();

    /**
     * Closes the message client and releases all resources.
     *
     * @throws IOException If an error occurs during cleanup
     */
    @Override
    void close() throws IOException;
}
