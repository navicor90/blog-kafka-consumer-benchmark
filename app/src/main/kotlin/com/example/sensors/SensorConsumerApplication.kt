package com.example.sensors

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class SensorConsumerApplication

fun main(args: Array<String>) {
    runApplication<SensorConsumerApplication>(*args)
}
