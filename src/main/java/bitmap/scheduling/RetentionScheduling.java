package bitmap.scheduling;

import bitmap.service.AppLaunchService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;

@Component
@Async
public class RetentionScheduling {

    @Resource
    private AppLaunchService appLaunchService;

    @Scheduled(cron = "0 0 0,4 * * ?")
    public void scheduleTaskWithFixedRate() {
        appLaunchService.statisticsRetention(LocalDate.now().minusDays(1));
        appLaunchService.statisticsRetention(LocalDate.now().minusDays(2));
    }
}
