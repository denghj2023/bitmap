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
@RequestMapping
public class HeartbeatController {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisTemplate<String, LocalDateTime> redisTemplate;

    /**
     * Use Redis bitmap to record the heartbeat of the device,
     * determine offset according to the current time,
     * and set the corresponding bit to 1.
     */
    @PostMapping("/heartbeat")
    public Object appLaunch(@RequestBody EventDTO eventDTO) {
        LocalDateTime eventTime = eventDTO.getEventTime();
        String deviceId = eventDTO.getDeviceId();

        // Get first launch time of th device.
        String key = String.format(AppLaunchController.KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        LocalDateTime firstLaunchTime = redisTemplate.opsForValue().get(key);
        if (firstLaunchTime != null) {
            // Calculate the offset.
            long offset = Duration.between(firstLaunchTime, eventTime).toMinutes();
            log.debug("Device {} heartbeat at {}", deviceId, offset);

            // Set the corresponding bit to 1.
            stringRedisTemplate.opsForValue().setBit(deviceId, offset, true);
        }

        return MapUtil.builder()
                .put("code", 0)
                .put("msg", "SUCCESS")
                .build();
    }
}
