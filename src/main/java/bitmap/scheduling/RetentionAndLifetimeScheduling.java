package bitmap.scheduling;

import bitmap.service.AppLaunchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;

@Slf4j
@Async
@Component
public class RetentionAndLifetimeScheduling {

    @Resource
    private AppLaunchService appLaunchService;

    @Scheduled(cron = "0 0 0,4 * * ?")
    public void statisticsRetentionAndLifetime() {
        log.info("Retention scheduling start.");
        long start = System.currentTimeMillis();
        appLaunchService.statisticsRetentionAndLifetime(LocalDate.now().minusDays(1));
        log.info("Retention scheduling end, cost: {}s.", (System.currentTimeMillis() - start) / 1000);
    }
}
