package com.craftcodehouse.promotions.accumulator

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Consumed, KStream, KTable, Produced}
import java.util
import java.util.Locale
import java.util.Properties
import java.util.concurrent.CountDownLatch

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import java.util.Collections
import java.util

//val personAvroStream: KStream<String, GenericRecord> = streamsBuilder
//.stream(personsAvroTopic, Consumed.with(Serdes.String(), avroSerde))


case class Person(customerID: String, email: String, firstName: String)
case class GameStake(game: String, action: String, customerId: String, stake: Int)

case class Person(customerID: String, stake: Int)

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







/**
  * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
  * that computes a simple word occurrence histogram from an input text.
  * <p>
  * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
  * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
  * is an updated count of a single word.
  * <p>
  * Before running this example you must create the input topic and the output topic (e.g. via
  * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
  * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
  */



//    val builder = new StreamsBuilder
//    val textLines = builder.stream(keyGenericAvroSerde, valueGenericAvroSerde, "my-avro-topic")

//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)


// setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
// Note: To re-run the demo, you need to use the offset reset tool:
// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
