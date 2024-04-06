package bitmap;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;

@Slf4j
@SpringBootTest
class BitMapTest {

    @Resource
    RedisTemplate<String, LocalDateTime> redisTemplate;

    @Test
    void contextLoads() {
        LocalDateTime now = LocalDateTime.now();
        redisTemplate.opsForValue().set("test.now", now);
        LocalDateTime now2 = redisTemplate.opsForValue().get("test.now");
        Assertions.assertEquals(now, now2);
    }
}
