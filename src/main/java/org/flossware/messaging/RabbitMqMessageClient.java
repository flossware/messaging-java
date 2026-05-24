package org.flossware.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * MessageClient implementation for RabbitMQ.
 *
 * <p>Supports reading and writing byte messages to RabbitMQ queues using AMQP protocol.
 * Each key maps to a separate queue.</p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * MessageClient rabbitmq = RabbitMqMessageClient.builder()
 *     .host("localhost")
 *     .port(5672)
 *     .username("guest")
 *     .password("guest")
 *     .queuePrefix("msg.")
 *     .build();
 *
 * // Write message (publishes to queue)
 * rabbitmq.write("events", "event data".getBytes());
 *
 * // Read message (consumes from queue)
 * byte[] data = rabbitmq.read("events");
 *
 * rabbitmq.close();
 * }</pre>
 */
public class RabbitMqMessageClient implements MessageClient {
    private final Connection connection;
    private final Channel channel;
    private final String queuePrefix;

    private RabbitMqMessageClient(Connection connection, String queuePrefix) throws IOException {
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.queuePrefix = queuePrefix != null ? queuePrefix : "";
        this.channel = connection.createChannel();
    }

    private String buildQueueName(String key) {
        return queuePrefix + key;
    }

    private void ensureQueueExists(String queueName) throws IOException {
        try {
            // Declare queue (idempotent - creates if doesn't exist)
            channel.queueDeclare(queueName, true, false, false, null);
        } catch (IOException e) {
            throw new IOException("Failed to declare queue: " + queueName, e);
        }
    }

    @Override
    public void write(String key, byte[] value) throws IOException {
        String queueName = buildQueueName(key);
        ensureQueueExists(queueName);

        try {
            channel.basicPublish("", queueName, null, value);
        } catch (IOException e) {
            throw new IOException("Failed to publish message to RabbitMQ: " + key, e);
        }
    }

    @Override
    public byte[] read(String key) throws IOException {
        String queueName = buildQueueName(key);
        ensureQueueExists(queueName);

        try {
            GetResponse response = channel.basicGet(queueName, true);
            return response != null ? response.getBody() : null;
        } catch (IOException e) {
            throw new IOException("Failed to read message from RabbitMQ: " + key, e);
        }
    }

    @Override
    public boolean exists(String key) throws IOException {
        String queueName = buildQueueName(key);

        try {
            AMQP.Queue.DeclareOk ok = channel.queueDeclarePassive(queueName);
            return ok != null && ok.getMessageCount() > 0;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public List<String> listKeys(String prefix) throws IOException {
        throw new UnsupportedOperationException(
            "RabbitMQ does not support queue listing via AMQP protocol. " +
            "Use RabbitMQ Management HTTP API for queue enumeration."
        );
    }

    @Override
    public void delete(String key) throws IOException {
        String queueName = buildQueueName(key);

        try {
            channel.queueDelete(queueName);
        } catch (IOException e) {
            throw new IOException("Failed to delete queue from RabbitMQ: " + key, e);
        }
    }

    @Override
    public String getDescription() {
        return "RabbitMQ[prefix=" + queuePrefix + "]";
    }

    @Override
    public void close() throws IOException {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (TimeoutException e) {
            throw new IOException("Failed to close RabbitMQ connection", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 5672;
        private String username = "guest";
        private String password = "guest";
        private String virtualHost = "/";
        private String queuePrefix = "";

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder virtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder queuePrefix(String queuePrefix) {
            this.queuePrefix = queuePrefix;
            return this;
        }

        public RabbitMqMessageClient build() throws IOException {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualHost);

            try {
                Connection connection = factory.newConnection();
                return new RabbitMqMessageClient(connection, queuePrefix);
            } catch (IOException e) {
                throw e;
            } catch (TimeoutException e) {
                throw new IOException("Connection to RabbitMQ timed out", e);
            } catch (Exception e) {
                throw new IOException("Failed to connect to RabbitMQ", e);
            }
        }
    }
}
