package bitmap;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class BitMap {

    public static void main(String[] args) {
        try {
            SpringApplication.run(BitMap.class, args);
        } catch (Exception e) {
            log.error("Error starting BitMap", e);
        }
    }
}
