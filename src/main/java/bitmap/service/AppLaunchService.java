package bitmap.service;

import bitmap.dto.EventDTO;

import java.time.LocalDate;
import java.time.ZonedDateTime;

public interface AppLaunchService {

    /**
     * Record the first launch time of the device.
     *
     * @param eventDTO eventDTO
     */
    void recordFirstLaunchTime(EventDTO eventDTO);

    /**
     * Get the first launch time of the device.
     *
     * @param deviceId deviceId
     * @return first launch time of the device
     */
    ZonedDateTime getFirstLaunchTime(String deviceId);

    /**
     * Record app launch event when first launch.
     *
     * @param eventDTO eventDTO
     */
    void recordFirstLaunch(EventDTO eventDTO);

    /**
     * Get the first launch event of the device.
     *
     * @param deviceId deviceId
     * @return first launch event of the device
     */
    EventDTO getFirstLaunch(String deviceId);

    /**
     * Record app launch event every day.
     *
     * @param eventDTO eventDTO
     */
    void recordDailyLaunch(EventDTO eventDTO);

    /**
     * Get the daily launch event of the device.
     *
     * @param deviceId   deviceId
     * @param launchDate launch date
     * @return daily launch event of the device
     */
    EventDTO getDailyLaunch(String deviceId, LocalDate launchDate);
}
