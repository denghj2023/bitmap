package bitmap.scheduling;

import bitmap.service.HeartbeatService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;

@Component
@Async
public class SessionDurationScheduling {

    @Resource
    private HeartbeatService heartbeatService;

    @Scheduled(cron = "0 0 0,4 * * ?")
    public void scheduleTaskWithFixedRate() {
        heartbeatService.statisticsSessionDuration(LocalDate.now().minusDays(1));
        heartbeatService.statisticsSessionDuration(LocalDate.now().minusDays(2));
    }
}
