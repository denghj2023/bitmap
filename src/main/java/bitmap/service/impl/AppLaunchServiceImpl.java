package bitmap.service.impl;

import bitmap.dto.EventDTO;
import bitmap.service.AppLaunchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Service
public class AppLaunchServiceImpl implements AppLaunchService {

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

    @Override
    public void recordFirstLaunchTime(EventDTO eventDTO) {
        String deviceId = eventDTO.getDeviceId();
        ZonedDateTime eventTime = eventDTO.getEventTime();

        // If the key does not exist, set the value.
        String key = String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        if (redisTemplate.opsForValue().get(key) == null) {
            redisTemplate.opsForValue().set(key, eventTime.toLocalDateTime());
        }
    }

    @Override
    public ZonedDateTime getFirstLaunchTime(String deviceId) {
        String key = String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        return Optional.ofNullable(redisTemplate.opsForValue().get(key))
                .map(localDateTime -> localDateTime.atZone(ZoneId.systemDefault()))
                .orElse(null);
    }

    @Override
    public void recordFirstLaunch(EventDTO eventDTO) {
        // If exists, return.
        String documentId = eventDTO.getDeviceId();
        if (elasticsearchRestTemplate.exists(documentId, IndexCoordinates.of("first_app_launch"))) {
            return;
        }

        try {
            eventDTO.put("first_launch_time", eventDTO.getEventTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            eventDTO.put("device_id", eventDTO.getDeviceId());

            // Index to Elasticsearch.
            IndexQuery indexQuery = new IndexQueryBuilder()
                    .withId(documentId)
                    .withObject(eventDTO)
                    .build();
            elasticsearchRestTemplate.index(indexQuery, IndexCoordinates.of("first_app_launch"));
        } finally {
            eventDTO.remove("first_launch_time");
            eventDTO.remove("device_id");
        }
    }

    @Override
    public EventDTO getFirstLaunch(String deviceId) {
        return elasticsearchRestTemplate.get(deviceId,
                EventDTO.class,
                IndexCoordinates.of("first_app_launch"));
    }

    @Override
    public void recordDailyLaunch(EventDTO eventDTO) {
        String deviceId = eventDTO.getDeviceId();
        ZonedDateTime eventTime = eventDTO.getEventTime();

        // If exists, return.
        String documentId = deviceId + "_" + eventTime.toLocalDate().format(DateTimeFormatter.ISO_DATE);
        if (elasticsearchRestTemplate.exists(documentId, IndexCoordinates.of("daily_app_launch_unique"))) {
            return;
        }

        // Get first launch time of the device.
        String keyOfFirstLaunchTime = String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        LocalDateTime firstLaunchTime = redisTemplate.opsForValue().get(keyOfFirstLaunchTime);
        Objects.requireNonNull(firstLaunchTime, "First launch time must not be null");

        /*
         * Index to Elasticsearch.
         */
        try {
            eventDTO.put("first_launch_time", firstLaunchTime
                    .atZone(ZoneId.systemDefault())
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            eventDTO.put("launch_time", eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            eventDTO.put("device_id", deviceId);

            // Index to Elasticsearch.
            IndexQuery indexQuery = new IndexQueryBuilder()
                    .withId(documentId)
                    .withObject(eventDTO)
                    .build();
            elasticsearchRestTemplate.index(indexQuery, IndexCoordinates.of("daily_app_launch_unique"));
        } finally {
            eventDTO.remove("first_launch_time");
            eventDTO.remove("launch_time");
            eventDTO.remove("device_id");
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

    @Override
    public EventDTO getDailyLaunch(String deviceId, LocalDate launchDate) {
        return elasticsearchRestTemplate.get(deviceId + "_" + launchDate,
                EventDTO.class,
                IndexCoordinates.of("daily_app_launch_unique"));
    }
}
