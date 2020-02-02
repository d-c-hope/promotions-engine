package com.craftcodehouse.promotions.accumulator

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream._
import java.util
import java.util.Locale
import java.util.Properties
import java.util.concurrent.CountDownLatch

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import java.util.Collections
import java.util



case class Person(customerID: String, email: String, firstName: String)
case class GameStake(game: String, action: String, customerId: String, stake: Int)


object BetsAccumulator {

//  def doSetup():  Unit = {
//
//  }

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-promotions")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde)
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde)
    props.put("schema.registry.url", "http://my-schema-registry:8081")


    val avroSerde = new GenericAvroSerde
    val jmap = new java.util.HashMap[String, String]()
    jmap.put("schema.registry.url", "http://localhost:8081")
    avroSerde.configure(jmap, false)
//    avroSerde.configure(Map("schema.registry.url" -> "http://localhost:8081"), false))
//    {
//      configure(mapOf(Pair("schema.registry.url", "http://localhost:8081")), false)
//    }
    val streamsBuilder = new StreamsBuilder
    val con = Consumed.`with`(Serdes.String(), avroSerde)
//    streamsBuilder.stream()
//    Consumed.`with`(Serdes.String(Serdes.String(), avroSerde))

//    val personAvroStream: KStream<String, GenericRecord> = streamsBuilder.stream("test-topic-1", Consumed.`with`(Serdes.String(Serdes.String(), avroSerde)))

    val personAvroStream: KStream[String, GenericRecord] = streamsBuilder.stream("test-topic-1", Consumed.`with`(Serdes.String(), avroSerde))

//      .stream("test-topic-1", Consumed.with(Serdes.String(), avroSerde))
//      val vm: ValueMapper[GenericRecord, Person] = personA => Person("david", 34)
//      val personStream: KStream[String, Person] = personAvroStream.mapValues(vm)


    val personStream: KStream[String, Person] = personAvroStream.mapValues {
      personA => Person("david", 34)
    }

    personStream.print(Printed.toSysOut())


    val streams = new KafkaStreams(streamsBuilder.build, props)

    val latch = new CountDownLatch(1)
    // attach shutdown handler to catch control-c
    Runtime.getRuntime.addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
      override def run(): Unit = {
        streams.close
        latch.countDown()
      }
    })
    try {
      streams.start
      latch.await()
    } catch {
      case e: Throwable =>
        System.exit(1)
    }
    System.exit(0)
  }
}


class BetsAccumulator {

}



