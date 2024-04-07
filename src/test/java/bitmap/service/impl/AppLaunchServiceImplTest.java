package bitmap.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.time.LocalDate;

@Slf4j
@SpringBootTest
class AppLaunchServiceImplTest {

    @Resource
    AppLaunchServiceImpl appLaunchService;

    @Test
    void statisticsRetentionAndLifetime() {
        appLaunchService.statisticsRetentionAndLifetime(LocalDate.now());
    }
}
