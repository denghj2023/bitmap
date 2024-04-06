package bitmap;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import javax.annotation.Resource;
import java.time.LocalDate;
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
class AppLaunchControllerTest {

    @Resource
    ElasticsearchRestTemplate elasticsearchRestTemplate;
    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Test
    void test(@Autowired MockMvc mvc) throws Exception {
        // App launch.
        ZonedDateTime requestTime = ZonedDateTime.now();
        Map<String, Object> map = new HashMap<>();
        map.put("platform", "android");
        map.put("androidid", UUID.randomUUID().toString());
        map.put("event_time_offset_sec", 5);
        map.put("request_time", requestTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        mvc.perform(post("/events/app_launch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));

        // Validate: get first_app_launch from ES.
        String idOfFirstAppLaunch = map.get("platform") + "_" + map.get("androidid");
        EventDTO firstAppLaunch = elasticsearchRestTemplate.get(idOfFirstAppLaunch,
                EventDTO.class,
                IndexCoordinates.of("first_app_launch"));
        log.info("firstAppLaunch: {}", JSON.toJSONString(firstAppLaunch, JSONWriter.Feature.PrettyFormat));
        Assertions.assertNotNull(firstAppLaunch);

        // Validate: get daily_app_launch_unique from ES.
        String idOfAppLaunch = map.get("platform") + "_" + map.get("androidid") + "_" + LocalDate.now();
        EventDTO appLaunch = elasticsearchRestTemplate.get(idOfAppLaunch,
                EventDTO.class,
                IndexCoordinates.of("daily_app_launch_unique"));
        log.debug("appLaunch: {}", JSON.toJSONString(appLaunch, JSONWriter.Feature.PrettyFormat));
        Assertions.assertNotNull(appLaunch);

        // ------------------------------

        // App launch.
        map.put("request_time", requestTime.plusDays(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        mvc.perform(post("/events/app_launch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));

        // Validate: get first_app_launch from ES.
        idOfFirstAppLaunch = map.get("platform") + "_" + map.get("androidid");
        firstAppLaunch = elasticsearchRestTemplate.get(idOfFirstAppLaunch,
                EventDTO.class,
                IndexCoordinates.of("first_app_launch"));
        log.info("firstAppLaunch: {}", JSON.toJSONString(firstAppLaunch, JSONWriter.Feature.PrettyFormat));
        Assertions.assertNotNull(firstAppLaunch);
        Assertions.assertEquals(requestTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                firstAppLaunch.get("request_time").toString());

        // Validate: get daily_app_launch_unique from ES.
        idOfAppLaunch = map.get("platform") + "_" + map.get("androidid") + "_" + LocalDate.now().plusDays(1);
        appLaunch = elasticsearchRestTemplate.get(idOfAppLaunch,
                EventDTO.class,
                IndexCoordinates.of("daily_app_launch_unique"));
        log.debug("appLaunch: {}", JSON.toJSONString(appLaunch, JSONWriter.Feature.PrettyFormat));
        Assertions.assertNotNull(appLaunch);

        // ------------------------------
        // Validate: get daily launch count from Redis.
        String keyOfDailyLaunch = String.format(AppLaunchController.KEY_OF_LAUNCH_PER_DAY, appLaunch.getDeviceId());
        Long count = stringRedisTemplate.execute((RedisConnection connection)
                -> connection.bitCount(keyOfDailyLaunch.getBytes()));
        Assertions.assertEquals(2, count);
    }
}