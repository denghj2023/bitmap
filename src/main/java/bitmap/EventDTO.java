package bitmap;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
        ZonedDateTime requestTime = Optional.ofNullable(get("request_time"))
                .map(Object::toString)
                // 2024-04-05T02:43:00+08:00
                .map(s -> ZonedDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .orElse(null);

        Long eventTimeOffsetSec = Optional.ofNullable(get("event_time_offset_sec"))
                .map(Object::toString)
                .map(Long::parseLong)
                .orElse(0L);

        return requestTime != null ? requestTime.plusSeconds(eventTimeOffsetSec) : null;
    }
}
