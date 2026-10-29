package com.example.sensors

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

@Component
class ThroughputReporter(
    @Value("\${benchmark.expected-messages:0}") private val expectedMessages: Long,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val processed = AtomicLong()
    private val startedAt = AtomicLong()
    private val finished = AtomicBoolean()

    fun markStart() {
        startedAt.compareAndSet(0, System.nanoTime())
    }

    fun record(count: Int) {
        val total = processed.addAndGet(count.toLong())
        if (expectedMessages in 1..total && finished.compareAndSet(false, true)) {
            val elapsedMs = elapsedMs()
            log.info("RESULT messages={} durationMs={} throughput={}", total, elapsedMs, rate(total, elapsedMs))
        }
    }

    @Scheduled(fixedRate = 5000)
    fun report() {
        if (startedAt.get() == 0L || finished.get()) return
        val total = processed.get()
        val elapsedMs = elapsedMs()
        log.info("PROGRESS messages={} durationMs={} throughput={}", total, elapsedMs, rate(total, elapsedMs))
    }

    private fun elapsedMs() = (System.nanoTime() - startedAt.get()) / 1_000_000

    private fun rate(total: Long, elapsedMs: Long) = if (elapsedMs == 0L) 0 else total * 1000 / elapsedMs
}
