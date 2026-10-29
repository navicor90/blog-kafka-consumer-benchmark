package com.example.sensors.optimized

import com.example.sensors.SensorReading
import com.example.sensors.ThroughputReporter
import com.example.sensors.retrying
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.util.concurrent.Executors
import java.util.function.Consumer

/** Batch consumption + discard + dedupe + skip unchanged + parallel chunks with one transaction each. */
@Configuration
@Profile("optimized")
class OptimizedConsumer {

    @Bean
    fun sensorReadings(processor: OptimizedBatchProcessor, reporter: ThroughputReporter): Consumer<List<SensorReading>> =
        Consumer { readings ->
            reporter.markStart()
            processor.process(readings)
            reporter.record(readings.size)
        }
}

@Component
@Profile("optimized")
class OptimizedBatchProcessor(
    private val jdbc: JdbcTemplate,
    private val namedJdbc: NamedParameterJdbcTemplate,
    private val transactionTemplate: TransactionTemplate,
    @Value("\${benchmark.parallelism}") private val parallelism: Int,
    @Value("\${benchmark.max-attempts:3}") private val maxAttempts: Int,
) {
    private val dispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()

    // Sensor status rarely changes, so it is cached instead of read per message
    private val decommissioned: Set<Long> =
        jdbc.queryForList("SELECT id FROM sensor WHERE decommissioned", Long::class.java).toHashSet()

    fun process(readings: List<SensorReading>) {
        // Records arrive in offset order per partition, so the last one per key is the newest
        val latest = readings
            .filterNot { it.sensorId in decommissioned }
            .associateBy { it.sensorId }
        if (latest.isEmpty()) return

        val current = currentTemperatures(latest.keys)
        val changed = latest.values.filter { current[it.sensorId] != it.payload.temperature }
        if (changed.isEmpty()) return

        runBlocking {
            changed
                .groupBy { Math.floorMod(it.sensorId, parallelism) }
                .map { (_, chunk) ->
                    launch(dispatcher) {
                        retrying(maxAttempts) {
                            transactionTemplate.executeWithoutResult { write(chunk) }
                        }
                    }
                }
                .joinAll()
        }
    }

    private fun currentTemperatures(ids: Collection<Long>): Map<Long, Double?> {
        val result = HashMap<Long, Double?>(ids.size)
        namedJdbc.query("SELECT id, temperature FROM sensor WHERE id IN (:ids)", mapOf("ids" to ids)) { rs ->
            result[rs.getLong(1)] = rs.getDouble(2).takeUnless { rs.wasNull() }
        }
        return result
    }

    private fun write(chunk: List<SensorReading>) {
        jdbc.batchUpdate(
            "UPDATE sensor SET temperature = ?, updated_at = now() WHERE id = ?",
            chunk.map { arrayOf<Any>(it.payload.temperature, it.sensorId) },
        )
    }
}
