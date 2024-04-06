package bitmap.service.impl;

import bitmap.dto.EventDTO;
import bitmap.service.AppLaunchService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    @Override
    public void statisticsRetention(LocalDate activeDate) {
        // Query daily launch event of the device from ES, and calculate the retention of device.
        this.queryDailyLaunchEvent(activeDate, this::calculateRetentionOfDevice);
    }

    // Query daily launch event of the device from ES.
    private void queryDailyLaunchEvent(LocalDate activeDate, Consumer<List<EventDTO>> consumer) {
        ZonedDateTime start = activeDate.atStartOfDay(ZoneId.systemDefault());
        int pageSize = 500;
        int currentPage = 0;

        while (true) {
            NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
            queryBuilder.withQuery(QueryBuilders.boolQuery().filter(
                    QueryBuilders.rangeQuery("launch_time")
                            .gte(start)
                            .lt(start.plusDays(1))
            ));
            queryBuilder.withPageable(PageRequest.of(currentPage, pageSize));
            NativeSearchQuery searchQuery = queryBuilder.build();
            SearchHits<EventDTO> searchHits = elasticsearchRestTemplate.search(searchQuery,
                    EventDTO.class,
                    IndexCoordinates.of("daily_app_launch_unique"));
            List<EventDTO> results = searchHits.stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

            // Consume the results.
            consumer.accept(results);

            if (results.size() < pageSize) {
                break;
            }

            currentPage++;
        }
    }

    // Calculate the retention of device.
    private void calculateRetentionOfDevice(List<EventDTO> eventDTOS) {
        List<UpdateQuery> queries = new ArrayList<>(eventDTOS.size());
        for (EventDTO eventDTO : eventDTOS) {
            // Calculate retention of device.
            String key = String.format(KEY_OF_LAUNCH_PER_DAY, eventDTO.getDeviceId());

            // Update the session duration to ES.
            EventDTO entity = new EventDTO();
            entity.put("retention_day_1",
                    Boolean.TRUE.equals(stringRedisTemplate.opsForValue().getBit(key, 1)) ? 1 : 0);
            entity.put("retention_day_2",
                    Boolean.TRUE.equals(stringRedisTemplate.opsForValue().getBit(key, 2)) ? 1 : 0);
            entity.put("retention_day_3",
                    Boolean.TRUE.equals(stringRedisTemplate.opsForValue().getBit(key, 3)) ? 1 : 0);
            entity.put("retention_day_7",
                    Boolean.TRUE.equals(stringRedisTemplate.opsForValue().getBit(key, 7)) ? 1 : 0);
            entity.put("retention_day_15",
                    Boolean.TRUE.equals(stringRedisTemplate.opsForValue().getBit(key, 15)) ? 1 : 0);

            String documentId = eventDTO.getDeviceId();
            UpdateQuery updateQuery = UpdateQuery.builder(documentId)
                    .withDocument(Document.from(entity))
                    .build();
            queries.add(updateQuery);
        }

        elasticsearchRestTemplate.bulkUpdate(queries, IndexCoordinates.of("first_app_launch"));
    }
}
