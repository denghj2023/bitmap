package bitmap.service.impl;

import bitmap.dto.EventDTO;
import bitmap.service.AppLaunchService;
import bitmap.service.HeartbeatService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@Service
public class HeartbeatServiceImpl implements HeartbeatService {

    /**
     * Key of heartbeat record of the device.
     */
    public static final String KEY_OF_HEARTBEAT_PER_MINUTE = "device:%s:heartbeat_per_minute";
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private AppLaunchService appLaunchService;
    @Resource
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Override
    public void receiveHeartbeat(EventDTO eventDTO) {
        LocalDateTime eventTime = eventDTO.getEventTime().toLocalDateTime();
        String deviceId = eventDTO.getDeviceId();

        // Get first launch time of th device.
        ZonedDateTime firstLaunchTime = appLaunchService.getFirstLaunchTime(deviceId);

        if (firstLaunchTime != null) {
            // Calculate the offset.
            LocalDateTime start = firstLaunchTime.toLocalDateTime()
                    .withHour(0)
                    .withMinute(0)
                    .withSecond(0)
                    .withNano(0);
            long offset = Duration.between(start, eventTime).toMinutes();

            // Present the offset as day:hour:minute.
            String offsetStr = String.format("%d:%d:%d(%d)",
                    offset / 1440,
                    (offset % 1440) / 60,
                    offset % 60,
                    offset);
            log.debug("Device {} heartbeat at {}", deviceId, offsetStr);

            // Set the corresponding bit to 1.
            String keyOfHeartbeat = String.format(KEY_OF_HEARTBEAT_PER_MINUTE, deviceId);
            stringRedisTemplate.opsForValue().setBit(keyOfHeartbeat, offset, true);
        }
    }

    @Override
    public void statisticsSessionDuration(LocalDate activeDate) {
        // Query daily launch event of the device from ES.
        this.queryDailyLaunchEvent(activeDate, eventDTO -> {
            // Calculate the session duration of device.
            this.calculateSessionDurationOfDevice(eventDTO);

            // Calculate the session duration of daily active device.
            this.calculateSessionDurationOfDailyActiveDevice(eventDTO, activeDate);
        });
    }

    // Query daily launch event of the device from ES.
    private void queryDailyLaunchEvent(LocalDate activeDate, Consumer<EventDTO> consumer) {
        ZonedDateTime start = activeDate.atStartOfDay(ZoneId.systemDefault());
        int pageSize = 1000;
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

            for (EventDTO result : results) {
                consumer.accept(result);
            }

            if (results.size() < pageSize) {
                break;
            }

            currentPage++;
        }
    }

    // Calculate the session duration of device.
    private void calculateSessionDurationOfDevice(EventDTO eventDTO) {
        // Calculate the session duration by counting the bits.
        String key = String.format(KEY_OF_HEARTBEAT_PER_MINUTE, eventDTO.getDeviceId());
        Long sessionDuration = stringRedisTemplate.execute((RedisConnection connection)
                -> connection.bitCount(key.getBytes()));
        log.debug("Device {} session duration: {}", eventDTO.getDeviceId(), sessionDuration);

        // Update the session duration to ES.
        EventDTO entity = new EventDTO();
        entity.put("session_duration", sessionDuration);

        String documentId = eventDTO.getDeviceId();
        UpdateQuery updateQuery = UpdateQuery.builder(documentId)
                .withDocument(Document.from(entity))
                .build();
        elasticsearchRestTemplate.update(updateQuery, IndexCoordinates.of("first_app_launch"));
    }

    // Calculate the session duration of daily active device.
    private void calculateSessionDurationOfDailyActiveDevice(EventDTO eventDTO, LocalDate activeDate) {
        // Calculate offset between first launch time and launch time.
        LocalDateTime firstLaunchTimeStr = Optional.ofNullable(eventDTO.get("first_launch_time"))
                .map(Object::toString)
                .map(s -> LocalDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .orElseThrow(() -> new IllegalArgumentException("First launch time must not be null."));

        LocalDateTime launchTimeStr = Optional.ofNullable(eventDTO.get("launch_time"))
                .map(Object::toString)
                .map(s -> LocalDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .orElseThrow(() -> new IllegalArgumentException("Launch time must not be null."));

        long days = Duration.between(firstLaunchTimeStr, launchTimeStr).toDays();
        long start = days * 180;
        long end = days * 180 + 179;

        // Calculate the session duration by counting the bits.
        String key = String.format(KEY_OF_HEARTBEAT_PER_MINUTE, eventDTO.getDeviceId());
        Long sessionDuration = stringRedisTemplate.execute((RedisConnection connection)
                -> connection.bitCount(key.getBytes(), start, end));
        log.debug("Device {} session duration: {}", eventDTO.getDeviceId(), sessionDuration);

        // Update the session duration to ES.
        EventDTO entity = new EventDTO();
        entity.put("session_duration", sessionDuration);

        String documentId = eventDTO.getDeviceId() + "_" + activeDate.format(DateTimeFormatter.ISO_DATE);
        UpdateQuery updateQuery = UpdateQuery.builder(documentId)
                .withDocument(Document.from(entity))
                .build();
        elasticsearchRestTemplate.update(updateQuery, IndexCoordinates.of("daily_app_launch_unique"));
    }
}
