package bitmap;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
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
                    .map(Object::toString)
                    .orElse(null);
        } else if ("ios".equalsIgnoreCase(platform)) {
            return Optional.ofNullable(get("idfv"))
                    .map(Object::toString)
                    .orElse(null);
        } else {
            return null;
        }
    }

    public LocalDateTime getEventTime() {
        LocalDateTime eventTime = Optional.ofNullable(get("event_time"))
                .map(Object::toString)
                // 2024-04-05T02:43:00+08:00
                .map(s -> LocalDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                .orElse(null);

        Long eventTimeOffsetSec = Optional.ofNullable(get("event_time_offset_sec"))
                .map(Object::toString)
                .map(Long::parseLong)
                .orElse(0L);

        return eventTime != null ? eventTime.plusSeconds(eventTimeOffsetSec) : null;
    }
}
