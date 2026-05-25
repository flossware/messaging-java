package org.flossware.messaging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for RedisMessageClient to achieve 100% coverage.
 */
class RedisMessageClientTest {

    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    private AutoCloseable mocks;
    private RedisMessageClient client;

    @BeforeEach
    void setUp() {
        mocks = MockitoAnnotations.openMocks(this);
        when(jedisPool.getResource()).thenReturn(jedis);
        when(jedisPool.isClosed()).thenReturn(false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    @DisplayName("Should support builder chaining")
    void testBuilderChaining() {
        RedisMessageClient.Builder builder = RedisMessageClient.builder();
        assertNotNull(builder);

        assertSame(builder, builder.host("localhost"));
        assertSame(builder, builder.port(6379));
        assertSame(builder, builder.password("secret"));
        assertSame(builder, builder.database(0));
        assertSame(builder, builder.keyPrefix("test:"));
        assertSame(builder, builder.timeout(5000));
        assertSame(builder, builder.poolConfig(new JedisPoolConfig()));
    }

    @Test
    @DisplayName("Should build with password")
    void testBuilderBuildWithPassword() {
        RedisMessageClient.Builder builder = RedisMessageClient.builder()
            .host("localhost")
            .port(6379)
            .password("secret")
            .database(0)
            .keyPrefix("test:")
            .timeout(100);

        // Build will succeed (JedisPool is created lazily)
        // but operations will fail without real Redis
        RedisMessageClient client = builder.build();
        assertNotNull(client);
        assertDoesNotThrow(client::close);
    }

    @Test
    @DisplayName("Should build without password")
    void testBuilderBuildWithoutPassword() {
        RedisMessageClient.Builder builder = RedisMessageClient.builder()
            .host("localhost")
            .port(6379)
            .database(0)
            .keyPrefix("test:")
            .timeout(100);

        // Build will succeed (covers the password == null branch)
        RedisMessageClient client = builder.build();
        assertNotNull(client);
        assertDoesNotThrow(client::close);
    }

    @Test
    @DisplayName("Should build with custom pool config")
    void testBuilderBuildWithPoolConfig() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);

        RedisMessageClient.Builder builder = RedisMessageClient.builder()
            .host("localhost")
            .poolConfig(poolConfig)
            .timeout(100);

        // Build will succeed (covers the poolConfig != null branch)
        RedisMessageClient client = builder.build();
        assertNotNull(client);
        assertDoesNotThrow(client::close);
    }

    @Test
    @DisplayName("Should build with null pool config")
    void testBuilderBuildWithNullPoolConfig() {
        RedisMessageClient.Builder builder = RedisMessageClient.builder()
            .host("localhost")
            .timeout(100);

        // poolConfig is null, should create new JedisPoolConfig()
        RedisMessageClient client = builder.build();
        assertNotNull(client);
        assertDoesNotThrow(client::close);
    }

    @Test
    @DisplayName("Should write message successfully")
    void testWriteSuccess() throws Exception {
        client = createTestClient("");

        byte[] value = "test-data".getBytes();
        client.write("key1", value);

        verify(jedis).set("key1".getBytes(), value);
        verify(jedis).close();
    }

    @Test
    @DisplayName("Should write message with key prefix")
    void testWriteWithPrefix() throws Exception {
        client = createTestClient("prefix:");

        byte[] value = "test-data".getBytes();
        client.write("key1", value);

        verify(jedis).set("prefix:key1".getBytes(), value);
    }

    @Test
    @DisplayName("Should throw IOException on write failure")
    void testWriteFailure() throws Exception {
        client = createTestClient("");

        when(jedis.set(any(byte[].class), any(byte[].class)))
            .thenThrow(new RuntimeException("Redis error"));

        IOException exception = assertThrows(IOException.class,
            () -> client.write("key1", "data".getBytes()));

        assertTrue(exception.getMessage().contains("Failed to write message to Redis: key1"));
        assertNotNull(exception.getCause());
    }

    @Test
    @DisplayName("Should read message successfully")
    void testReadSuccess() throws Exception {
        client = createTestClient("");

        byte[] expectedValue = "test-data".getBytes();
        when(jedis.get("key1".getBytes())).thenReturn(expectedValue);

        byte[] result = client.read("key1");

        assertArrayEquals(expectedValue, result);
        verify(jedis).get("key1".getBytes());
    }

    @Test
    @DisplayName("Should read message with key prefix")
    void testReadWithPrefix() throws Exception {
        client = createTestClient("prefix:");

        byte[] expectedValue = "test-data".getBytes();
        when(jedis.get("prefix:key1".getBytes())).thenReturn(expectedValue);

        byte[] result = client.read("key1");

        assertArrayEquals(expectedValue, result);
        verify(jedis).get("prefix:key1".getBytes());
    }

    @Test
    @DisplayName("Should return null when key not found")
    void testReadNotFound() throws Exception {
        client = createTestClient("");

        when(jedis.get("missing".getBytes())).thenReturn(null);

        byte[] result = client.read("missing");

        assertNull(result);
    }

    @Test
    @DisplayName("Should throw IOException on read failure")
    void testReadFailure() throws Exception {
        client = createTestClient("");

        when(jedis.get(any(byte[].class)))
            .thenThrow(new RuntimeException("Redis error"));

        IOException exception = assertThrows(IOException.class,
            () -> client.read("key1"));

        assertTrue(exception.getMessage().contains("Failed to read message from Redis: key1"));
    }

    @Test
    @DisplayName("Should check existence successfully")
    void testExistsTrue() throws Exception {
        client = createTestClient("");

        when(jedis.exists("key1".getBytes())).thenReturn(true);

        assertTrue(client.exists("key1"));
        verify(jedis).exists("key1".getBytes());
    }

    @Test
    @DisplayName("Should return false when key doesn't exist")
    void testExistsFalse() throws Exception {
        client = createTestClient("");

        when(jedis.exists("missing".getBytes())).thenReturn(false);

        assertFalse(client.exists("missing"));
    }

    @Test
    @DisplayName("Should throw IOException on exists failure")
    void testExistsFailure() throws Exception {
        client = createTestClient("");

        when(jedis.exists(any(byte[].class)))
            .thenThrow(new RuntimeException("Redis error"));

        IOException exception = assertThrows(IOException.class,
            () -> client.exists("key1"));

        assertTrue(exception.getMessage().contains("Failed to check existence in Redis: key1"));
    }

    @Test
    @DisplayName("Should list keys without prefix")
    void testListKeysNoPrefix() throws Exception {
        client = createTestClient("");

        ScanResult<String> scanResult = new ScanResult<>(
            ScanParams.SCAN_POINTER_START,
            Arrays.asList("key1", "key2", "key3")
        );

        when(jedis.scan(anyString(), any(ScanParams.class)))
            .thenReturn(scanResult);

        List<String> keys = client.listKeys("key");

        assertEquals(3, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
        assertTrue(keys.contains("key3"));
    }

    @Test
    @DisplayName("Should list keys with prefix removed")
    void testListKeysWithPrefix() throws Exception {
        client = createTestClient("app:");

        ScanResult<String> scanResult = new ScanResult<>(
            ScanParams.SCAN_POINTER_START,
            Arrays.asList("app:user1", "app:user2")
        );

        when(jedis.scan(anyString(), any(ScanParams.class)))
            .thenReturn(scanResult);

        List<String> keys = client.listKeys("user");

        assertEquals(2, keys.size());
        assertTrue(keys.contains("user1"));
        assertTrue(keys.contains("user2"));
    }

    @Test
    @DisplayName("Should handle keys without prefix in listKeys")
    void testListKeysWithoutExpectedPrefix() throws Exception {
        client = createTestClient("app:");

        ScanResult<String> scanResult = new ScanResult<>(
            ScanParams.SCAN_POINTER_START,
            Arrays.asList("other:key1")
        );

        when(jedis.scan(anyString(), any(ScanParams.class)))
            .thenReturn(scanResult);

        List<String> keys = client.listKeys("other");

        assertEquals(1, keys.size());
        assertTrue(keys.contains("other:key1"));
    }

    @Test
    @DisplayName("Should handle multiple scan iterations")
    void testListKeysMultipleScanIterations() throws Exception {
        client = createTestClient("");

        ScanResult<String> firstScan = new ScanResult<>("cursor1", Arrays.asList("key1", "key2"));
        ScanResult<String> secondScan = new ScanResult<>(ScanParams.SCAN_POINTER_START, Arrays.asList("key3"));

        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
            .thenReturn(firstScan);
        when(jedis.scan(eq("cursor1"), any(ScanParams.class)))
            .thenReturn(secondScan);

        List<String> keys = client.listKeys("key");

        assertEquals(3, keys.size());
        verify(jedis, times(2)).scan(anyString(), any(ScanParams.class));
    }

    @Test
    @DisplayName("Should throw IOException on listKeys failure")
    void testListKeysFailure() throws Exception {
        client = createTestClient("");

        when(jedis.scan(anyString(), any(ScanParams.class)))
            .thenThrow(new RuntimeException("Redis error"));

        IOException exception = assertThrows(IOException.class,
            () -> client.listKeys("key"));

        assertTrue(exception.getMessage().contains("Failed to list keys from Redis: key"));
    }

    @Test
    @DisplayName("Should delete key successfully")
    void testDeleteSuccess() throws Exception {
        client = createTestClient("");

        client.delete("key1");

        verify(jedis).del("key1".getBytes());
    }

    @Test
    @DisplayName("Should delete key with prefix")
    void testDeleteWithPrefix() throws Exception {
        client = createTestClient("prefix:");

        client.delete("key1");

        verify(jedis).del("prefix:key1".getBytes());
    }

    @Test
    @DisplayName("Should throw IOException on delete failure")
    void testDeleteFailure() throws Exception {
        client = createTestClient("");

        doThrow(new RuntimeException("Redis error"))
            .when(jedis).del(any(byte[].class));

        IOException exception = assertThrows(IOException.class,
            () -> client.delete("key1"));

        assertTrue(exception.getMessage().contains("Failed to delete message from Redis: key1"));
    }

    @Test
    @DisplayName("Should return description without prefix")
    void testGetDescriptionNoPrefix() throws Exception {
        client = createTestClient("");

        assertEquals("Redis[prefix=]", client.getDescription());
    }

    @Test
    @DisplayName("Should return description with prefix")
    void testGetDescriptionWithPrefix() throws Exception {
        client = createTestClient("app:");

        assertEquals("Redis[prefix=app:]", client.getDescription());
    }

    @Test
    @DisplayName("Should close pool when not closed")
    void testCloseSuccess() throws Exception {
        client = createTestClient("");

        when(jedisPool.isClosed()).thenReturn(false);

        client.close();

        verify(jedisPool).close();
    }

    @Test
    @DisplayName("Should not close already closed pool")
    void testCloseAlreadyClosed() throws Exception {
        client = createTestClient("");

        when(jedisPool.isClosed()).thenReturn(true);

        client.close();

        verify(jedisPool, never()).close();
    }

    @Test
    @DisplayName("Should handle null pool in close")
    void testCloseNullPool() throws Exception {
        // Create client with reflection to set null pool
        client = createTestClient("");

        java.lang.reflect.Field poolField = RedisMessageClient.class.getDeclaredField("jedisPool");
        poolField.setAccessible(true);
        poolField.set(client, null);

        assertDoesNotThrow(() -> client.close());
    }

    @Test
    @DisplayName("Should throw NullPointerException when JedisPool is null in constructor")
    void testConstructorNullPool() throws Exception {
        java.lang.reflect.Constructor<RedisMessageClient> constructor =
            RedisMessageClient.class.getDeclaredConstructor(JedisPool.class, String.class);
        constructor.setAccessible(true);

        java.lang.reflect.InvocationTargetException exception = assertThrows(
            java.lang.reflect.InvocationTargetException.class,
            () -> constructor.newInstance(null, "prefix:")
        );

        assertTrue(exception.getCause() instanceof NullPointerException);
        assertTrue(exception.getCause().getMessage().contains("jedisPool cannot be null"));
    }

    @Test
    @DisplayName("Should handle null keyPrefix in constructor")
    void testConstructorNullPrefix() throws Exception {
        java.lang.reflect.Constructor<RedisMessageClient> constructor =
            RedisMessageClient.class.getDeclaredConstructor(JedisPool.class, String.class);
        constructor.setAccessible(true);

        RedisMessageClient testClient = constructor.newInstance(jedisPool, null);

        assertEquals("Redis[prefix=]", testClient.getDescription());
    }

    /**
     * Helper method to create a RedisMessageClient with mocked JedisPool.
     */
    private RedisMessageClient createTestClient(String prefix) throws Exception {
        java.lang.reflect.Constructor<RedisMessageClient> constructor =
            RedisMessageClient.class.getDeclaredConstructor(JedisPool.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(jedisPool, prefix);
    }
}
