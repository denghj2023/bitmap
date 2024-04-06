package bitmap;

import cn.hutool.core.map.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * App launch controller
 */
@Slf4j
@RestController
@RequestMapping("/events")
public class AppLaunchController {

    /**
     * Key of first launch time.
     */
    public static final String KEY_OF_FIRST_LAUNCH_TIME = "device:%s:first_launch_time";
    /**
     * Key of launch record of the device.
     */
    public static final String KEY_OF_LAUNCH_PER_DAY = "device:%s:launch_per_day";
    @Resource
    private RedisTemplate<String, LocalDateTime> redisTemplate;
    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 1. Record the first launch time of the device.
     * 2. Record app launch event when first launch.
     * 3. Record app launch event every day.
     * 4. Use Redis bitmap to record the daily launch record of the device.
     */
    @PostMapping("/app_launch")
    public Object appLaunch(@RequestBody EventDTO eventDTO) {
        // Record the first launch time of the device.
        this.recordFirstLaunchTime(eventDTO);

        // Record app launch event when first launch.
        this.recordFirstLaunch(eventDTO);

        // Record app launch event every day.
        this.recordDailyLaunch(eventDTO);

        return MapUtil.builder()
                .put("code", 0)
                .put("msg", "SUCCESS")
                .build();
    }

    // Record the first launch time of the device.
    private void recordFirstLaunchTime(EventDTO eventDTO) {
        String deviceId = eventDTO.getDeviceId(); // Device ID
        ZonedDateTime eventTime = eventDTO.getEventTime(); // Event time

        // If the key does not exist, set the value.
        String key = String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        if (redisTemplate.opsForValue().get(key) == null) {
            redisTemplate.opsForValue().set(key, eventTime.toLocalDateTime());
        }
    }

    // Record app launch event when first launch.
    private void recordFirstLaunch(EventDTO eventDTO) {
        // If exists, return.
        String documentId = eventDTO.getDeviceId();
        if (elasticsearchRestTemplate.exists(documentId, IndexCoordinates.of("first_app_launch"))) {
            return;
        }

        try {
            eventDTO.put("first_launch_time", eventDTO.getEventTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

            // Index to Elasticsearch.
            IndexQuery indexQuery = new IndexQueryBuilder()
                    .withId(documentId)
                    .withObject(eventDTO)
                    .build();
            elasticsearchRestTemplate.index(indexQuery, IndexCoordinates.of("first_app_launch"));
        } finally {
            eventDTO.remove("first_launch_time");
        }
    }

    // Record app launch event every day.
    private void recordDailyLaunch(EventDTO eventDTO) {
        String deviceId = eventDTO.getDeviceId();
        ZonedDateTime eventTime = eventDTO.getEventTime();

        // If exists, return.
        String documentId = deviceId + "_" + eventTime.toLocalDate();
        if (elasticsearchRestTemplate.exists(documentId, IndexCoordinates.of("daily_app_launch_unique"))) {
            return;
        }

        // Get first launch time of the device.
        String keyOfFirstLaunchTime = String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        LocalDateTime firstLaunchTime = redisTemplate.opsForValue().get(keyOfFirstLaunchTime);
        Objects.requireNonNull(firstLaunchTime, "First launch time must not be null");

        try {
            eventDTO.put("first_launch_time", firstLaunchTime
                    .atZone(ZoneId.systemDefault())
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            eventDTO.put("launch_time", eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

            // Index to Elasticsearch.
            IndexQuery indexQuery = new IndexQueryBuilder()
                    .withId(documentId)
                    .withObject(eventDTO)
                    .build();
            elasticsearchRestTemplate.index(indexQuery, IndexCoordinates.of("daily_app_launch_unique"));
        } finally {
            eventDTO.remove("first_launch_time");
            eventDTO.remove("launch_time");
        }

        /*
         * Use Redis bitmap to record the daily launch record of the device.
         */
        String keyOfLaunchPerDay = String.format(KEY_OF_LAUNCH_PER_DAY, deviceId);

        // Calculate the offset.
        long offset = Duration.between(firstLaunchTime, eventTime).toDays();

        // Present the offset as day.
        String offsetStr = String.format("%d", offset);
        log.debug("Device {} launch at {}", deviceId, offsetStr);

        // Set the corresponding bit to 1.
        stringRedisTemplate.opsForValue().setBit(keyOfLaunchPerDay, offset, true);
    }
}
