package bitmap;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@SpringBootTest
class BitMapTest {

    @Resource
    RedisTemplate<String, LocalDateTime> redisTemplate;
    @Resource
    ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Test
    void contextLoads() {
        LocalDateTime now = LocalDateTime.now();
        redisTemplate.opsForValue().set("test.now", now);
        LocalDateTime now2 = redisTemplate.opsForValue().get("test.now");
        Assertions.assertEquals(now, now2);

        Object mapping = elasticsearchRestTemplate.indexOps(IndexCoordinates.of("user-event-2024.04.04"))
                .getMapping();
        log.debug("mapping: {}", JSON.toJSONString(mapping, JSONWriter.Feature.PrettyFormat));
    }
}
