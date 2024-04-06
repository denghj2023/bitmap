package bitmap;

import bitmap.service.impl.HeartbeatServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import javax.annotation.Resource;
import java.util.UUID;

@Slf4j
@SpringBootTest
class RedisBitTest {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Test
    void test() {
        String key = UUID.randomUUID().toString();
        stringRedisTemplate.opsForValue().setBit(key, 1, true);
        stringRedisTemplate.opsForValue().setBit(key, 3, true);
        stringRedisTemplate.opsForValue().setBit(key, 5, true);
        stringRedisTemplate.opsForValue().setBit(key, 50, true);

        // Get the value of the bit at offset 5.
        log.info("Bit at offset 5: {}", stringRedisTemplate.opsForValue().getBit(key, 5));

        // Get all the bits as a binary string
        ValueOperations<String, String> valueOps = stringRedisTemplate.opsForValue();
        String storedValue = valueOps.get(key);
        Assertions.assertNotNull(storedValue);
        StringBuilder bits = new StringBuilder();
        for (char ch : storedValue.toCharArray()) {
            bits.append(String.format("%8s", Integer.toBinaryString(ch)).replace(' ', '0'));
        }
        log.info("All bits: {}", bits);

        // Use execute to perform BITCOUNT.
        Long bitsSet = stringRedisTemplate.execute((RedisConnection connection) -> connection.bitCount(key.getBytes()));
        log.info("Number of bits set to 1: {}", bitsSet);
        Assertions.assertEquals(4, bitsSet);

        // Delete the key.
        stringRedisTemplate.delete(key);
    }

    @Test
    void test2() {
        String deviceId = "android_f2eaff29-4a76-4f8b-a24f-c9cfb77ba8f9";
        String key = String.format(HeartbeatServiceImpl.KEY_OF_HEARTBEAT_PER_MINUTE, deviceId);
        stringRedisTemplate.opsForValue().setBit(key, 1, true);

        // Use execute to perform BITCOUNT.
        Long bitsSet = stringRedisTemplate.execute((RedisConnection connection) -> connection.bitCount(key.getBytes()));
        log.info("Number of bits set to 1: {}", bitsSet);
    }
}
