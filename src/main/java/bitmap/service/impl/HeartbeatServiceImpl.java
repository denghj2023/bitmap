package bitmap.service.impl;

import bitmap.dto.AverageSessionDurationDTO;
import bitmap.dto.EventDTO;
import bitmap.service.AppLaunchService;
import bitmap.service.HeartbeatService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

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

            // Present the offset as hour:minute.
            String offsetStr = String.format("%d(%d:%d)", offset, offset / 60, offset % 60);
            log.debug("Device {} heartbeat at {}", deviceId, offsetStr);

            // Set the corresponding bit to 1.
            String keyOfHeartbeat = String.format(KEY_OF_HEARTBEAT_PER_MINUTE, deviceId);
            stringRedisTemplate.opsForValue().setBit(keyOfHeartbeat, offset, true);
        }
    }

    @Override
    public AverageSessionDurationDTO statisticsAverageSessionDuration(LocalDate activeDate) {
        return null;
    }
}
