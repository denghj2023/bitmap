package bitmap.scheduling;

import bitmap.service.HeartbeatService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;

@Slf4j
@Async
@Component
public class SessionDurationScheduling {

    @Resource
    private HeartbeatService heartbeatService;

    @Scheduled(cron = "0 0 0,4 * * ?")
    public void statisticsSessionDuration() {
        log.info("Session duration scheduling start.");
        long start = System.currentTimeMillis();
        heartbeatService.statisticsSessionDuration(LocalDate.now().minusDays(1));
        log.info("Session duration scheduling end, cost: {}s.", (System.currentTimeMillis() - start) / 1000);
    }
}
