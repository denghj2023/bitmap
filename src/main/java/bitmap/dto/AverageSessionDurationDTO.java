package bitmap.dto;

import lombok.Data;

import java.time.LocalDate;

/**
 * Average session duration DTO
 */
@Data
public class AverageSessionDurationDTO {

    /**
     * First launch date of device.
     */
    private LocalDate firstLaunchDate;
    /**
     * Active date of device.
     */
    private LocalDate activeDate;
    /**
     * Total active users.
     */
    private Integer totalActiveUsers;
    /**
     * Total session duration.
     */
    private Long sessionDurationMin;
}
