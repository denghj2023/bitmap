package bitmap;

import cn.hutool.core.map.MapUtil;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.LocalDateTime;

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

    private void recordDailyLaunch(EventDTO eventDTO) {
    }

    private void recordFirstLaunch(EventDTO eventDTO) {
    }

    private void recordFirstLaunchTime(EventDTO eventDTO) {
        String deviceId = eventDTO.getDeviceId();
        LocalDateTime eventTime = eventDTO.getEventTime();
        String key = String.format(KEY_OF_FIRST_LAUNCH_TIME, deviceId);
        redisTemplate.opsForValue().setIfAbsent(key, eventTime);
    }
}
