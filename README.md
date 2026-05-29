# JMessaging

Universal messaging and cache abstraction library for Java. Provides a simple, unified API for reading and writing byte messages to Apache Kafka, RabbitMQ, and Redis.

## Features

- ✅ **Unified API** - Single interface for all messaging systems
- ✅ **3 Systems** - Apache Kafka, RabbitMQ, Redis
- ✅ **Builder Pattern** - Fluent, type-safe configuration
- ✅ **Optional Dependencies** - Include only the systems you need
- ✅ **Thread-Safe** - Concurrent operations supported
- ✅ **AutoCloseable** - Proper resource management
- ✅ **Minimal Dependencies** - Java 11+, system libraries are optional

## Quick Start

### Maven Dependency

```xml
<dependency>
    <groupId>org.flossware</groupId>
    <artifactId>messaging-java</artifactId>
    <version>1.0</version>
</dependency>

<!-- Add system SDK (only include what you need) -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>4.3.0</version>
</dependency>
```

### Basic Usage

```java
import org.flossware.messaging.MessageClient;
import org.flossware.messaging.KafkaMessageClient;

// Create Kafka client
MessageClient client = KafkaMessageClient.builder()
    .bootstrapServers("localhost:9092")
    .topic("events")
    .groupId("my-consumer-group")
    .build();

// Write a message
client.write("user-123", "event data".getBytes());

// Read a message
byte[] data = client.read("user-123");

// Check if message exists
if (client.exists("user-123")) {
    System.out.println("Message exists!");
}

// Clean up
client.close();
```

## Supported Systems

### Apache Kafka

```java
MessageClient kafka = KafkaMessageClient.builder()
    .bootstrapServers("localhost:9092")
    .topic("events")
    .groupId("consumer-group-1")
    .pollTimeout(1000)
    .build();
```

**Features:**
- Producer and consumer in one client
- Internal message caching
- Configurable poll timeout
- Auto-generated group ID if not specified

**Dependency:**
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>4.3.0</version>
</dependency>
```

### RabbitMQ

```java
MessageClient rabbitmq = RabbitMqMessageClient.builder()
    .host("localhost")
    .port(5672)
    .username("guest")
    .password("guest")
    .queuePrefix("msg.")
    .build();
```

**Features:**
- AMQP protocol
- Automatic queue creation
- Durable queues
- Virtual host support

**Dependency:**
```xml
<dependency>
    <groupId>com.rabbitmq</groupId>
    <artifactId>amqp-client</artifactId>
    <version>5.30.0</version>
</dependency>
```

### Redis

```java
MessageClient redis = RedisMessageClient.builder()
    .host("localhost")
    .port(6379)
    .password("secret")
    .database(0)
    .keyPrefix("msg:")
    .build();
```

**Features:**
- Connection pooling (Jedis)
- Key prefix support
- Pattern-based key listing
- Database selection

**Dependency:**
```xml
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>7.5.0</version>
</dependency>
```

## API Reference

```java
public interface MessageClient extends AutoCloseable {
    void write(String key, byte[] value) throws IOException;
    byte[] read(String key) throws IOException;
    boolean exists(String key) throws IOException;
    List<String> listKeys(String prefix) throws IOException;  // Not supported by all
    void delete(String key) throws IOException;
    String getDescription();
    void close() throws IOException;
}
```

## Common Use Cases

### Event Streaming

```java
// Kafka for high-throughput event streaming
MessageClient events = KafkaMessageClient.builder()
    .bootstrapServers("kafka.example.com:9092")
    .topic("user-events")
    .groupId("event-processor")
    .build();

events.write("user-login", eventData);
```

### Task Queues

```java
// RabbitMQ for reliable task queuing
MessageClient tasks = RabbitMqMessageClient.builder()
    .host("rabbitmq.example.com")
    .username("worker")
    .password("secret")
    .queuePrefix("tasks.")
    .build();

tasks.write("email-queue", taskData);
byte[] task = tasks.read("email-queue");
```

### Caching

```java
// Redis for high-speed caching
MessageClient cache = RedisMessageClient.builder()
    .host("redis.example.com")
    .keyPrefix("cache:")
    .build();

cache.write("user-123", userData);
byte[] cached = cache.read("user-123");
```

### Multi-System Architecture

```java
// Write to Kafka, cache in Redis
MessageClient kafka = KafkaMessageClient.builder()
    .bootstrapServers("localhost:9092")
    .topic("events")
    .build();

MessageClient redis = RedisMessageClient.builder()
    .host("localhost")
    .keyPrefix("cache:")
    .build();

byte[] data = "important event".getBytes();
kafka.write("event-1", data);   // Persist to event stream
redis.write("event-1", data);   // Cache for fast access
```

## System Comparison

| System | Speed | Durability | Use Case | Listing |
|--------|-------|------------|----------|---------|
| **Kafka** | ⚡⚡⚡ | ⭐⭐⭐⭐⭐ | Event streaming, logs | ❌ No |
| **RabbitMQ** | ⚡⚡⚡⚡ | ⭐⭐⭐⭐ | Task queues, RPC | ❌ No* |
| **Redis** | ⚡⚡⚡⚡⚡ | ⭐⭐ | Caching, sessions | ✅ Yes |

*RabbitMQ listing requires Management HTTP API

## Best Practices

1. **Always use try-with-resources or close():**
   ```java
   try (MessageClient client = builder.build()) {
       client.write("key", data);
   }
   ```

2. **Use appropriate system for your use case:**
   ```java
   // High-throughput events → Kafka
   // Reliable work queues → RabbitMQ
   // Fast caching → Redis
   ```

3. **Handle null responses:**
   ```java
   byte[] data = client.read("key");
   if (data == null) {
       // Key not found or no messages available
   }
   ```

4. **Use key prefixes to avoid collisions:**
   ```java
   .keyPrefix("myapp:")      // Redis
   .queuePrefix("myapp.")    // RabbitMQ
   ```

## Versioning and Releases

This project uses **X.Y semantic versioning** (e.g., 1.0, 1.1, 2.0). Versions are automatically incremented on commits to the main branch and published to packagecloud.io.

### Maven Repository

```xml
<repositories>
    <repository>
        <id>packagecloud-flossware</id>
        <url>https://packagecloud.io/flossware/java/maven2</url>
    </repository>
</repositories>
```

## Building from Source

```bash
git clone https://github.com/FlossWare/messaging-java.git
cd messaging-java
mvn clean install
```

## License

Apache License 2.0

## Related Projects

- [jcloudstorage](https://github.com/FlossWare/cloudstorage-java) - Cloud storage abstraction (S3, Azure, GCS, Google Drive, Dropbox, OneDrive)
- [jfiletransfer](https://github.com/FlossWare/filetransfer-java) - File transfer abstraction (SFTP, WebDAV, SMB/CIFS, FTP/FTPS)
- [jcontainer](https://github.com/FlossWare/container-java) - Container abstraction (Kubernetes, Docker, Hazelcast)
- [jvcs](https://github.com/FlossWare/vcs-java) - Version control abstraction (Git)
- [jclassloader](https://github.com/FlossWare/classloader-java) - Dynamic class loading from 34+ transport protocols
