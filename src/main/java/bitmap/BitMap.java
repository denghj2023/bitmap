package bitmap;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication
@EnableScheduling
@EnableAsync
public class BitMap {

    public static void main(String[] args) {
        try {
            SpringApplication.run(BitMap.class, args);
        } catch (Exception e) {
            log.error("Error starting BitMap", e);
        }
    }
}
