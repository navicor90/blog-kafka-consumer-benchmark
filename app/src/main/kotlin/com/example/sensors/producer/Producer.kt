package com.example.sensors.producer

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import java.io.StringReader
import java.sql.DriverManager
import kotlin.math.roundToLong
import kotlin.random.Random

/**
 * Seeds the sensor table and pre-fills the topic with a deterministic data set,
 * so both scenarios consume exactly the same messages.
 */
fun main() {
    (LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) as Logger).level = Level.WARN
    val sensors = env("SENSORS", "600000").toInt()
    val messages = env("MESSAGES", "2000000").toLong()
    val repeatProbability = env("REPEAT_PROBABILITY", "0.8").toDouble()
    val decommissionedRatio = env("DECOMMISSIONED_RATIO", "0.05").toDouble()
    val topic = env("TOPIC", "sensor-readings")
    val random = Random(env("SEED", "42").toLong())

    val temperatures = DoubleArray(sensors) { round1(15 + random.nextDouble() * 15) }
    val decommissioned = BooleanArray(sensors) { random.nextDouble() < decommissionedRatio }

    DriverManager.getConnection(env("DB_URL", "jdbc:postgresql://postgres:5432/sensors"), env("DB_USER", "sensors"), env("DB_PASSWORD", "sensors")).use { conn ->
        conn.createStatement().use { it.execute("TRUNCATE sensor, expected_state") }
        val copy = (conn.unwrap(PGConnection::class.java)).copyAPI
        val seed = StringBuilder()
        for (id in 0 until sensors) seed.append(id).append(',').append(temperatures[id]).append(',').append(decommissioned[id]).append('\n')
        copy.copyIn("COPY sensor (id, temperature, decommissioned) FROM STDIN WITH (FORMAT csv)", StringReader(seed.toString()))
        println("Seeded $sensors sensors (${decommissioned.count { it }} decommissioned)")

        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to env("KAFKA_BOOTSTRAP", "kafka:9092"),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "1",
            ProducerConfig.LINGER_MS_CONFIG to 20,
            ProducerConfig.BATCH_SIZE_CONFIG to 256 * 1024,
            ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
        )
        val startedAt = System.currentTimeMillis()
        KafkaProducer<String, String>(props).use { producer ->
            for (i in 1..messages) {
                val id = random.nextInt(sensors)
                if (random.nextDouble() >= repeatProbability) {
                    temperatures[id] = round1(temperatures[id] + if (random.nextBoolean()) 0.1 else -0.1)
                }
                val key = "sensor-$id"
                val value = """{"key":"$key","payload":{"temperature":${temperatures[id]}}}"""
                producer.send(ProducerRecord(topic, key, value))
                if (i % 250_000 == 0L) println("Produced $i/$messages messages")
            }
            producer.flush()
        }
        println("Produced $messages messages in ${System.currentTimeMillis() - startedAt} ms")

        val expected = StringBuilder()
        for (id in 0 until sensors) expected.append(id).append(',').append(temperatures[id]).append('\n')
        copy.copyIn("COPY expected_state (id, temperature) FROM STDIN WITH (FORMAT csv)", StringReader(expected.toString()))
        conn.createStatement().use { it.execute("ANALYZE") }
    }
}

private fun env(name: String, default: String) = System.getenv(name) ?: default

private fun round1(value: Double) = (value * 10).roundToLong() / 10.0
