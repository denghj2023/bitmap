package bitmap;

import cn.hutool.core.map.MapUtil;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * App launch controller
 */
@RestController
@RequestMapping("/events")
public class AppLaunchController {

    /**
     * Key of first launch time.
     */
    public static final String KEY_OF_FIRST_LAUNCH_TIME = "device:%s:first_launch_time";
    @Resource
    private RedisTemplate<String, LocalDateTime> redisTemplate;
    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    /**
     * 1. Record the first launch time of the device.
     * 2. Record app launch event when first launch.
     * 3. Record app launch event every day.
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
        String deviceId = eventDTO.getDeviceId();
        ZonedDateTime eventTime = eventDTO.getEventTime();
        String key = String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId);

        // If the key does not exist, set the value.
        if (redisTemplate.opsForValue().get(key) == null) {
            redisTemplate.opsForValue().set(key, eventTime.toLocalDateTime());
        }
    }

    // Record app launch event when first launch.
    private void recordFirstLaunch(EventDTO eventDTO) {
        try {
            eventDTO.put("first_launch_time", eventDTO.getEventTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            IndexQuery indexQuery = new IndexQuery();
            indexQuery.setObject(eventDTO);
            indexQuery.setId(eventDTO.getDeviceId());
            elasticsearchRestTemplate.index(indexQuery, IndexCoordinates.of("first_app_launch"));
        } finally {
            eventDTO.remove("first_launch_time");
        }
    }

    // Record app launch event every day.
    private void recordDailyLaunch(EventDTO eventDTO) {
        // Get first launch time of the device.
        String deviceId = eventDTO.getDeviceId();
        LocalDateTime firstLaunchTime = redisTemplate.opsForValue()
                .get(String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId));
        Objects.requireNonNull(firstLaunchTime, "First launch time must not be null");

        eventDTO.put("first_launch_time", firstLaunchTime
                .atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        eventDTO.put("launch_time", eventDTO.getEventTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        ZonedDateTime eventTime = eventDTO.getEventTime();
        String date = eventTime.format(DateTimeFormatter.ISO_LOCAL_DATE);
        String id = deviceId + "_" + date;

        IndexQuery indexQuery = new IndexQuery();
        indexQuery.setObject(eventDTO);
        indexQuery.setId(id);
        elasticsearchRestTemplate.index(indexQuery, IndexCoordinates.of("daily_app_launch_unique"));
    }
}
