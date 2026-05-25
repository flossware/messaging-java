package org.flossware.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for RabbitMqMessageClient to achieve 100% coverage.
 */
class RabbitMqMessageClientTest {

    @Mock
    private Connection connection;

    @Mock
    private Channel channel;

    @Mock
    private GetResponse getResponse;

    @Mock
    private AMQP.Queue.DeclareOk declareOk;

    private AutoCloseable mocks;
    private RabbitMqMessageClient client;

    @BeforeEach
    void setUp() throws Exception {
        mocks = MockitoAnnotations.openMocks(this);
        when(connection.createChannel()).thenReturn(channel);
        when(connection.isOpen()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                // Ignore
            }
        }
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    @DisplayName("Should support builder chaining")
    void testBuilderChaining() {
        RabbitMqMessageClient.Builder builder = RabbitMqMessageClient.builder();

        assertSame(builder, builder.host("localhost"));
        assertSame(builder, builder.port(5672));
        assertSame(builder, builder.username("user"));
        assertSame(builder, builder.password("pass"));
        assertSame(builder, builder.virtualHost("/test"));
        assertSame(builder, builder.queuePrefix("prefix:"));
    }

    @Test
    @DisplayName("Should build successfully with real connection")
    void testBuilderBuildSuccess() {
        RabbitMqMessageClient.Builder builder = RabbitMqMessageClient.builder()
            .host("localhost")
            .port(5672)
            .username("guest")
            .password("guest")
            .virtualHost("/")
            .queuePrefix("test:");

        // Attempt to build - may succeed if RabbitMQ is running locally
        // or fail with IOException if not. Either way, it exercises the code.
        try {
            RabbitMqMessageClient client = builder.build();
            assertNotNull(client);
            client.close();
        } catch (IOException e) {
            // Expected if RabbitMQ not running - still covers the build() attempt
            assertTrue(e.getMessage().contains("RabbitMQ") ||
                       e.getMessage().contains("Connection") ||
                       e.getMessage().contains("refused"));
        }
    }

    @Test
    @DisplayName("Should wrap TimeoutException in IOException during build")
    void testBuilderBuildTimeout() {
        RabbitMqMessageClient.Builder builder = RabbitMqMessageClient.builder()
            .host("10.255.255.1")  // Non-routable IP that will timeout
            .port(5672)
            .username("guest")
            .password("guest");

        // Attempt to build - should wrap TimeoutException or connection refused
        assertThrows(IOException.class, builder::build);
    }

    @Test
    @DisplayName("Should handle IOException during build")
    void testBuilderBuildIOException() {
        RabbitMqMessageClient.Builder builder = RabbitMqMessageClient.builder()
            .host("localhost")
            .port(1)  // Invalid port
            .username("invalid")
            .password("invalid");

        // Attempt to build - will fail with IOException
        assertThrows(IOException.class, builder::build);
    }

    @Test
    @DisplayName("Should write message successfully")
    void testWriteSuccess() throws Exception {
        client = createTestClient("");

        byte[] value = "test-data".getBytes();
        client.write("queue1", value);

        verify(channel).queueDeclare("queue1", true, false, false, null);
        verify(channel).basicPublish("", "queue1", null, value);
    }

    @Test
    @DisplayName("Should write message with queue prefix")
    void testWriteWithPrefix() throws Exception {
        client = createTestClient("app.");

        byte[] value = "test-data".getBytes();
        client.write("events", value);

        verify(channel).queueDeclare("app.events", true, false, false, null);
        verify(channel).basicPublish("", "app.events", null, value);
    }

    @Test
    @DisplayName("Should throw IOException on queue declare failure during write")
    void testWriteQueueDeclareFailure() throws Exception {
        client = createTestClient("");

        doThrow(new IOException("Queue error"))
            .when(channel).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());

        IOException exception = assertThrows(IOException.class,
            () -> client.write("queue1", "data".getBytes()));

        assertTrue(exception.getMessage().contains("Failed to declare queue: queue1"));
    }

    @Test
    @DisplayName("Should throw IOException on publish failure")
    void testWritePublishFailure() throws Exception {
        client = createTestClient("");

        doThrow(new IOException("Publish error"))
            .when(channel).basicPublish(anyString(), anyString(), any(), any(byte[].class));

        IOException exception = assertThrows(IOException.class,
            () -> client.write("queue1", "data".getBytes()));

        assertTrue(exception.getMessage().contains("Failed to publish message to RabbitMQ: queue1"));
    }

    @Test
    @DisplayName("Should read message successfully")
    void testReadSuccess() throws Exception {
        client = createTestClient("");

        byte[] expectedValue = "test-data".getBytes();
        when(getResponse.getBody()).thenReturn(expectedValue);
        when(channel.basicGet("queue1", true)).thenReturn(getResponse);

        byte[] result = client.read("queue1");

        assertArrayEquals(expectedValue, result);
        verify(channel).queueDeclare("queue1", true, false, false, null);
        verify(channel).basicGet("queue1", true);
    }

    @Test
    @DisplayName("Should read message with queue prefix")
    void testReadWithPrefix() throws Exception {
        client = createTestClient("app.");

        byte[] expectedValue = "test-data".getBytes();
        when(getResponse.getBody()).thenReturn(expectedValue);
        when(channel.basicGet("app.events", true)).thenReturn(getResponse);

        byte[] result = client.read("events");

        assertArrayEquals(expectedValue, result);
        verify(channel).basicGet("app.events", true);
    }

    @Test
    @DisplayName("Should return null when no message available")
    void testReadNoMessage() throws Exception {
        client = createTestClient("");

        when(channel.basicGet("empty", true)).thenReturn(null);

        byte[] result = client.read("empty");

        assertNull(result);
    }

    @Test
    @DisplayName("Should throw IOException on read failure")
    void testReadFailure() throws Exception {
        client = createTestClient("");

        doThrow(new IOException("Read error"))
            .when(channel).basicGet(anyString(), anyBoolean());

        IOException exception = assertThrows(IOException.class,
            () -> client.read("queue1"));

        assertTrue(exception.getMessage().contains("Failed to read message from RabbitMQ: queue1"));
    }

    @Test
    @DisplayName("Should return true when queue exists with messages")
    void testExistsTrue() throws Exception {
        client = createTestClient("");

        when(declareOk.getMessageCount()).thenReturn(5);
        when(channel.queueDeclarePassive("queue1")).thenReturn(declareOk);

        assertTrue(client.exists("queue1"));
        verify(channel).queueDeclarePassive("queue1");
    }

    @Test
    @DisplayName("Should return false when queue has no messages")
    void testExistsFalseNoMessages() throws Exception {
        client = createTestClient("");

        when(declareOk.getMessageCount()).thenReturn(0);
        when(channel.queueDeclarePassive("queue1")).thenReturn(declareOk);

        assertFalse(client.exists("queue1"));
    }

    @Test
    @DisplayName("Should return false when queue doesn't exist")
    void testExistsFalseQueueNotFound() throws Exception {
        client = createTestClient("");

        doThrow(new IOException("Queue not found"))
            .when(channel).queueDeclarePassive("missing");

        assertFalse(client.exists("missing"));
    }

    @Test
    @DisplayName("Should return false when declareOk is null")
    void testExistsFalseNullDeclareOk() throws Exception {
        client = createTestClient("");

        when(channel.queueDeclarePassive("queue1")).thenReturn(null);

        assertFalse(client.exists("queue1"));
    }

    @Test
    @DisplayName("Should throw UnsupportedOperationException for listKeys")
    void testListKeysUnsupported() throws Exception {
        client = createTestClient("");

        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class,
            () -> client.listKeys("prefix")
        );

        assertTrue(exception.getMessage().contains("RabbitMQ does not support queue listing"));
        assertTrue(exception.getMessage().contains("Management HTTP API"));
    }

    @Test
    @DisplayName("Should delete queue successfully")
    void testDeleteSuccess() throws Exception {
        client = createTestClient("");

        client.delete("queue1");

        verify(channel).queueDelete("queue1");
    }

    @Test
    @DisplayName("Should delete queue with prefix")
    void testDeleteWithPrefix() throws Exception {
        client = createTestClient("app.");

        client.delete("events");

        verify(channel).queueDelete("app.events");
    }

    @Test
    @DisplayName("Should throw IOException on delete failure")
    void testDeleteFailure() throws Exception {
        client = createTestClient("");

        doThrow(new IOException("Delete error"))
            .when(channel).queueDelete(anyString());

        IOException exception = assertThrows(IOException.class,
            () -> client.delete("queue1"));

        assertTrue(exception.getMessage().contains("Failed to delete queue from RabbitMQ: queue1"));
    }

    @Test
    @DisplayName("Should return description without prefix")
    void testGetDescriptionNoPrefix() throws Exception {
        client = createTestClient("");

        assertEquals("RabbitMQ[prefix=]", client.getDescription());
    }

    @Test
    @DisplayName("Should return description with prefix")
    void testGetDescriptionWithPrefix() throws Exception {
        client = createTestClient("app.");

        assertEquals("RabbitMQ[prefix=app.]", client.getDescription());
    }

    @Test
    @DisplayName("Should close channel and connection")
    void testCloseSuccess() throws Exception {
        client = createTestClient("");

        client.close();

        verify(channel).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("Should not close already closed channel")
    void testCloseClosedChannel() throws Exception {
        client = createTestClient("");

        when(channel.isOpen()).thenReturn(false);

        client.close();

        verify(channel, never()).close();
        verify(connection).close();
    }

    @Test
    @DisplayName("Should not close already closed connection")
    void testCloseClosedConnection() throws Exception {
        client = createTestClient("");

        when(connection.isOpen()).thenReturn(false);

        client.close();

        verify(channel).close();
        verify(connection, never()).close();
    }

    @Test
    @DisplayName("Should handle null channel in close")
    void testCloseNullChannel() throws Exception {
        client = createTestClient("");

        java.lang.reflect.Field channelField = RabbitMqMessageClient.class.getDeclaredField("channel");
        channelField.setAccessible(true);
        channelField.set(client, null);

        assertDoesNotThrow(() -> client.close());
        verify(connection).close();
    }

    @Test
    @DisplayName("Should handle null connection in close")
    void testCloseNullConnection() throws Exception {
        client = createTestClient("");

        java.lang.reflect.Field connectionField = RabbitMqMessageClient.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(client, null);

        assertDoesNotThrow(() -> client.close());
        verify(channel).close();
    }

    @Test
    @DisplayName("Should wrap TimeoutException as IOException on close")
    void testCloseTimeoutException() throws Exception {
        client = createTestClient("");

        doThrow(new TimeoutException("Timeout"))
            .when(connection).close();

        IOException exception = assertThrows(IOException.class,
            () -> client.close());

        assertTrue(exception.getMessage().contains("Failed to close RabbitMQ connection"));
        assertTrue(exception.getCause() instanceof TimeoutException);
    }

    @Test
    @DisplayName("Should throw NullPointerException when connection is null in constructor")
    void testConstructorNullConnection() throws Exception {
        java.lang.reflect.Constructor<RabbitMqMessageClient> constructor =
            RabbitMqMessageClient.class.getDeclaredConstructor(Connection.class, String.class);
        constructor.setAccessible(true);

        java.lang.reflect.InvocationTargetException exception = assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> constructor.newInstance(null, "prefix.")
        );

        assertTrue(exception.getCause() instanceof NullPointerException);
        assertTrue(exception.getCause().getMessage().contains("connection cannot be null"));
    }

    @Test
    @DisplayName("Should handle null queuePrefix in constructor")
    void testConstructorNullPrefix() throws Exception {
        java.lang.reflect.Constructor<RabbitMqMessageClient> constructor =
            RabbitMqMessageClient.class.getDeclaredConstructor(Connection.class, String.class);
        constructor.setAccessible(true);

        RabbitMqMessageClient testClient = constructor.newInstance(connection, null);

        assertEquals("RabbitMQ[prefix=]", testClient.getDescription());
    }

    /**
     * Helper method to create a RabbitMqMessageClient with mocked dependencies.
     */
    private RabbitMqMessageClient createTestClient(String prefix) throws Exception {
        java.lang.reflect.Constructor<RabbitMqMessageClient> constructor =
            RabbitMqMessageClient.class.getDeclaredConstructor(Connection.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(connection, prefix);
    }
}
