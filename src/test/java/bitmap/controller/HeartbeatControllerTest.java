package bitmap.controller;

import bitmap.service.impl.HeartbeatServiceImpl;
import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import javax.annotation.Resource;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
class HeartbeatControllerTest {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Test
    void test(@Autowired MockMvc mvc) throws Exception {
        ZonedDateTime requestTime = ZonedDateTime.now();
        Map<String, Object> map = new HashMap<>();
        map.put("platform", "android");
        map.put("androidid", UUID.randomUUID().toString());
        map.put("event_time_offset_sec", 5);
        map.put("request_time", requestTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        // App launch.
        mvc.perform(post("/app_launch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));

        // Heartbeat.
        mvc.perform(post("/heartbeat")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));

        // Validate: get total session duration from Redis.
        String deviceId = map.get("platform") + "_" + map.get("androidid");
        String key = String.format(HeartbeatServiceImpl.KEY_OF_HEARTBEAT_PER_MINUTE, deviceId);
        Long count = stringRedisTemplate.execute((RedisConnection connection)
                -> connection.bitCount(key.getBytes()));
        Assertions.assertEquals(1, count);

        // ---------------------
        // Heartbeat.
        requestTime = requestTime.plusMinutes(3);
        map.put("request_time", requestTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        mvc.perform(post("/heartbeat")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));

        // Validate: get total session duration from Redis.
        count = stringRedisTemplate.execute((RedisConnection connection)
                -> connection.bitCount(key.getBytes()));
        Assertions.assertEquals(2, count);

        // Get all the bits as a binary string
        String bitMap = stringRedisTemplate.opsForValue().get(key);
        Assertions.assertNotNull(bitMap);
        StringBuilder bits = new StringBuilder();
        for (char ch : bitMap.toCharArray()) {
            bits.append(String.format("%8s", Integer.toBinaryString(ch)).replace(' ', '0'));
        }
        log.debug("All bits: {}", bits);
        // Assertions.assertEquals("00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010", bits.toString());
    }

    @Test
    void test2(@Autowired MockMvc mvc) throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("platform", "android");
        map.put("androidid", UUID.randomUUID().toString());
        map.put("event_time_offset_sec", 5);
        map.put("request_time", ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        String deviceId = map.get("platform") + "_" + map.get("androidid");

        // App launch.
        mvc.perform(post("/app_launch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));

        // Heartbeat.
        this.heartbeat(mvc, map, ZonedDateTime.now());
        Long sessionDuration = this.bitCount(deviceId, 0, 179);
        Assertions.assertEquals(1, sessionDuration);

        // Heartbeat.
        this.heartbeat(mvc, map, ZonedDateTime.now().withHour(23).withMinute(59));
        sessionDuration = this.bitCount(deviceId, 0, 179);
        Assertions.assertEquals(2, sessionDuration);

        // Heartbeat.
        this.heartbeat(mvc, map, ZonedDateTime.now().plusDays(1).withHour(0).withMinute(0));
        sessionDuration = this.bitCount(deviceId, 180, 359);
        Assertions.assertEquals(1, sessionDuration);

        // Heartbeat.
        this.heartbeat(mvc, map, ZonedDateTime.now().plusDays(1).withHour(23).withMinute(59));
        sessionDuration = this.bitCount(deviceId, 180, 359);
        Assertions.assertEquals(2, sessionDuration);
    }

    private Long bitCount(String deviceId, long start, long end) {
        String key = String.format(HeartbeatServiceImpl.KEY_OF_HEARTBEAT_PER_MINUTE, deviceId);
        Long sessionDuration = stringRedisTemplate.execute((RedisConnection connection)
                -> connection.bitCount(key.getBytes(), start, end));
        log.debug("Device {} session duration: {}", deviceId, sessionDuration);
        return sessionDuration;
    }

    private void heartbeat(MockMvc mvc, Map<String, Object> map, ZonedDateTime withMinute) throws Exception {
        map.put("request_time", withMinute.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        mvc.perform(post("/heartbeat")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));
    }
}
