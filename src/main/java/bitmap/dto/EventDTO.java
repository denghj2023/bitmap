package bitmap.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Optional;

@Data
@EqualsAndHashCode(callSuper = true)
public class EventDTO extends HashMap<String, Object> {

    public String getDeviceId() {
        String platform = Optional.ofNullable(get("platform"))
                .map(Object::toString)
                .orElse(null);
        if (platform == null) {
            return null;
        }

        if ("android".equalsIgnoreCase(platform)) {
            return Optional.ofNullable(get("androidid"))
                    .map(s -> platform + "_" + s)
                    .orElse(null);
        } else if ("ios".equalsIgnoreCase(platform)) {
            return Optional.ofNullable(get("idfv"))
                    .map(s -> platform + "_" + s)
                    .orElse(null);
        } else {
            return null;
        }
    }

    public ZonedDateTime getEventTime() {
        ZonedDateTime requestTime = ZonedDateTime.now();

        Long eventTimeOffsetSec = Optional.ofNullable(get("event_time_offset_sec"))
                .map(Object::toString)
                .map(Long::parseLong)
                .orElse(0L);

        return requestTime.plusSeconds(eventTimeOffsetSec);
    }
}
