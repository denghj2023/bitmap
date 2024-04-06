package bitmap.controller;

import bitmap.dto.EventDTO;
import bitmap.service.AppLaunchService;
import cn.hutool.core.map.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.time.LocalDate;

/**
 * App launch controller
 */
@Slf4j
@RestController
@RequestMapping
public class AppLaunchController {

    @Resource
    private AppLaunchService appLaunchService;

    @PostMapping("/app_launch")
    public Object appLaunch(@RequestBody EventDTO eventDTO) {
        // Record the first launch time of the device.
        appLaunchService.recordFirstLaunchTime(eventDTO);

        // Record app launch event when first launch.
        appLaunchService.recordFirstLaunch(eventDTO);

        // Record app launch event every day.
        appLaunchService.recordDailyLaunch(eventDTO);

        return MapUtil.builder()
                .put("code", 0)
                .put("msg", "SUCCESS")
                .build();
    }

    @PostMapping("/stat-retention")
    public Object statRetention(@RequestParam LocalDate date) {
        appLaunchService.statisticsRetention(date);
        return MapUtil.builder()
                .put("code", 0)
                .put("msg", "SUCCESS")
                .build();
    }
}
