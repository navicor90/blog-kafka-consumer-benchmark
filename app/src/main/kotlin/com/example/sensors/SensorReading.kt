package com.example.sensors

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonIgnoreProperties

@JsonIgnoreProperties(ignoreUnknown = true)
data class SensorReading(
    val key: String,
    val payload: Payload,
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Payload(val temperature: Double)

    @get:JsonIgnore
    val sensorId: Long
        get() = key.removePrefix(KEY_PREFIX).toLong()

    companion object {
        const val KEY_PREFIX = "sensor-"
    }
}
