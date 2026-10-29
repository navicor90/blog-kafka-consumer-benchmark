package com.example.sensors

import org.slf4j.LoggerFactory
import org.springframework.dao.TransientDataAccessException

private val log = LoggerFactory.getLogger("com.example.sensors.Retry")

/** Retries [block] on transient DB errors; the last attempt lets the exception propagate. */
fun <T> retrying(maxAttempts: Int, block: () -> T): T {
    repeat(maxAttempts - 1) { attempt ->
        try {
            return block()
        } catch (e: TransientDataAccessException) {
            log.warn("[Reading] Chunk failed on attempt {}, retrying: {}", attempt + 1, e.message)
        }
    }
    return block()
}
