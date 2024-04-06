package bitmap;

import cn.hutool.core.map.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Heartbeat controller
 */
@Slf4j
@RestController
@RequestMapping("/events")
public class HeartbeatController {

    /**
     * Key of heartbeat record of the device.
     */
    public static final String KEY_OF_HEARTBEAT_PER_MINUTE = "device:%s:heartbeat_per_minute";
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisTemplate<String, LocalDateTime> redisTemplate;

    /**
     * Use Redis bitmap to record the heartbeat of the device,
     * determine offset according to the event time,
     * and set the corresponding bit to 1.
     */
    @PostMapping("/heartbeat")
    public Object appLaunch(@RequestBody EventDTO eventDTO) {
        LocalDateTime eventTime = eventDTO.getEventTime().toLocalDateTime();
        String deviceId = eventDTO.getDeviceId();

        // Get first launch time of th device.
        String key = String.format(AppLaunchController.KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        LocalDateTime firstLaunchTime = redisTemplate.opsForValue().get(key);

        if (firstLaunchTime != null) {
            // Calculate the offset.
            LocalDateTime start = firstLaunchTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
            long offset = Duration.between(start, eventTime).toMinutes();

            // Present the offset as hour:minute.
            String offsetStr = String.format("%d(%d:%d)", offset, offset / 60, offset % 60);
            log.debug("Device {} heartbeat at {}", deviceId, offsetStr);

            // Set the corresponding bit to 1.
            String keyOfHeartbeat = String.format(KEY_OF_HEARTBEAT_PER_MINUTE, deviceId);
            stringRedisTemplate.opsForValue().setBit(keyOfHeartbeat, offset, true);
        }

        return MapUtil.builder()
                .put("code", 0)
                .put("msg", "SUCCESS")
                .build();
    }
}
