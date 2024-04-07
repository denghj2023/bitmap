package bitmap.controller;

import bitmap.dto.EventDTO;
import bitmap.service.HeartbeatService;
import cn.hutool.core.map.MapUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.time.LocalDate;

/**
 * Heartbeat controller
 */
@Slf4j
@RestController
@RequestMapping
public class HeartbeatController {

    @Resource
    private HeartbeatService heartbeatService;

    @PostMapping("/heartbeat")
    public Object heartbeat(@RequestBody EventDTO eventDTO) {
        heartbeatService.receiveHeartbeat(eventDTO);

        return MapUtil.builder()
                .put("code", 0)
                .put("msg", "SUCCESS")
                .build();
    }

    @PostMapping("/stat-session-duration")
    public Object statSessionDuration(@RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate date) {
        heartbeatService.statisticsSessionDuration(date);
        return MapUtil.builder()
                .put("code", 0)
                .put("msg", "SUCCESS")
                .build();
    }
}
