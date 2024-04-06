package bitmap;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Slf4j
@SpringBootTest
@AutoConfigureMockMvc
class HeartbeatControllerTest {

    @Test
    void test(@Autowired MockMvc mvc) throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("platform", "android");
        map.put("androidid", UUID.randomUUID().toString());
        map.put("event_time_offset_sec", 5);
        map.put("event_time", "2024-04-05T02:43:00+08:00");
        mvc.perform(post("/heartbeat")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JSON.toJSONString(map)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(0))
                .andExpect(jsonPath("$.msg").value("SUCCESS"));
    }
}
