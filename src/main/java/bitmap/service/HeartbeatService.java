package bitmap.service;

import bitmap.dto.EventDTO;

import java.time.LocalDate;

public interface HeartbeatService {

    /**
     * Receive heartbeat event.
     *
     * @param eventDTO event data
     */
    void receiveHeartbeat(EventDTO eventDTO);

    /**
     * Statistics average session duration.
     *
     * @param activeDate active date
     */
    void statisticsSessionDuration(LocalDate activeDate);
}
