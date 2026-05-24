package org.flossware.messaging;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * MessageClient implementation for Redis.
 *
 * <p>Supports reading and writing byte messages to Redis using GET/SET operations.
 * Uses connection pooling for efficient resource management.</p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * MessageClient redis = RedisMessageClient.builder()
 *     .host("localhost")
 *     .port(6379)
 *     .password("secret")
 *     .keyPrefix("msg:")
 *     .build();
 *
 * // Write message
 * redis.write("user-123", "event data".getBytes());
 *
 * // Read message
 * byte[] data = redis.read("user-123");
 *
 * // List keys
 * List<String> keys = redis.listKeys("user-*");
 *
 * redis.close();
 * }</pre>
 */
public class RedisMessageClient implements MessageClient {
    private final JedisPool jedisPool;
    private final String keyPrefix;

    private RedisMessageClient(JedisPool jedisPool, String keyPrefix) {
        this.jedisPool = Objects.requireNonNull(jedisPool, "jedisPool cannot be null");
        this.keyPrefix = keyPrefix != null ? keyPrefix : "";
    }

    private String buildKey(String key) {
        return keyPrefix + key;
    }

    @Override
    public void write(String key, byte[] value) throws IOException {
        String fullKey = buildKey(key);

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(fullKey.getBytes(), value);
        } catch (Exception e) {
            throw new IOException("Failed to write message to Redis: " + key, e);
        }
    }

    @Override
    public byte[] read(String key) throws IOException {
        String fullKey = buildKey(key);

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(fullKey.getBytes());
        } catch (Exception e) {
            throw new IOException("Failed to read message from Redis: " + key, e);
        }
    }

    @Override
    public boolean exists(String key) throws IOException {
        String fullKey = buildKey(key);

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(fullKey.getBytes());
        } catch (Exception e) {
            throw new IOException("Failed to check existence in Redis: " + key, e);
        }
    }

    @Override
    public List<String> listKeys(String prefix) throws IOException {
        String pattern = buildKey(prefix) + "*";
        List<String> keys = new ArrayList<>();

        try (Jedis jedis = jedisPool.getResource()) {
            ScanParams scanParams = new ScanParams().match(pattern).count(100);
            String cursor = ScanParams.SCAN_POINTER_START;

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                List<String> matchedKeys = scanResult.getResult();

                // Remove prefix from keys
                for (String key : matchedKeys) {
                    if (key.startsWith(keyPrefix)) {
                        keys.add(key.substring(keyPrefix.length()));
                    } else {
                        keys.add(key);
                    }
                }

                cursor = scanResult.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));

            return keys;

        } catch (Exception e) {
            throw new IOException("Failed to list keys from Redis: " + prefix, e);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        String fullKey = buildKey(key);

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(fullKey.getBytes());
        } catch (Exception e) {
            throw new IOException("Failed to delete message from Redis: " + key, e);
        }
    }

    @Override
    public String getDescription() {
        return "Redis[prefix=" + keyPrefix + "]";
    }

    @Override
    public void close() throws IOException {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = "localhost";
        private int port = 6379;
        private String password;
        private int database = 0;
        private String keyPrefix = "";
        private int timeout = 2000;
        private JedisPoolConfig poolConfig;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder database(int database) {
            this.database = database;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
            return this;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder poolConfig(JedisPoolConfig poolConfig) {
            this.poolConfig = poolConfig;
            return this;
        }

        public RedisMessageClient build() {
            JedisPoolConfig config = poolConfig != null ? poolConfig : new JedisPoolConfig();

            JedisPool pool = password != null ?
                new JedisPool(config, host, port, timeout, password, database) :
                new JedisPool(config, host, port, timeout);

            return new RedisMessageClient(pool, keyPrefix);
        }
    }
}
