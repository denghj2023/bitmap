package bitmap.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.LocalDate;

@Slf4j
@SpringBootTest
class HeartbeatServiceImplTest {

    @Resource
    HeartbeatServiceImpl heartbeatService;

    @Test
    void statisticsAverageSessionDuration() {
        heartbeatService.statisticsSessionDuration(LocalDate.now());
    }
}
