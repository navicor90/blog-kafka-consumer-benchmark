package com.example.sensors.baseline

import com.example.sensors.SensorReading
import com.example.sensors.ThroughputReporter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.function.Consumer

/** One message at a time, one transaction per message, no work is discarded. */
@Configuration
@Profile("baseline")
class BaselineConsumer {

    @Bean
    fun sensorReadings(service: BaselineReadingService, reporter: ThroughputReporter): Consumer<SensorReading> =
        Consumer { reading ->
            reporter.markStart()
            service.process(reading)
            reporter.record(1)
        }
}

@Service
@Profile("baseline")
class BaselineReadingService(private val jdbc: JdbcTemplate) {

    @Transactional
    fun process(reading: SensorReading) {
        val decommissioned = jdbc.queryForObject(
            "SELECT decommissioned FROM sensor WHERE id = ?",
            Boolean::class.java,
            reading.sensorId,
        )
        if (decommissioned == true) return

        jdbc.update(
            "UPDATE sensor SET temperature = ?, updated_at = now() WHERE id = ?",
            reading.payload.temperature,
            reading.sensorId,
        )
    }
}
