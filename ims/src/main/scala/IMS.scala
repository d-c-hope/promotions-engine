package com.craftcodehouse.ims

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Produced, _}
import java.util.Properties
import java.util.concurrent.CountDownLatch


object IMS {

  def main(args: Array[String]): Unit = {

    println("\n*******************\nRunning the app\n\n\n")

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-imsx237596")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Integer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val streamsBuilder = new StreamsBuilder

    val serde: Serde[Profile_created] = Serdes.serdeFrom(new serdes.JsonSerializer[Profile_created],
      new serdes.JsonDeserializer[Profile_created])
    val profileCreatedStream = streamsBuilder.stream("test-topic-profilecreated1",
      Consumed.`with`(Serdes.String(), serde))

    val territoryPredicates = List(0,1,2).map { x=>
      val p: Predicate[String, Profile_created] = (key: String, value: Profile_created) => {
        if(value.territory == x) true else false
      }
      p
    }

    val usQueue = territoryPredicates(0)
    val gbQueue = territoryPredicates(1)
    val ieQueue = territoryPredicates(2)

    val streams = profileCreatedStream.branch(usQueue, gbQueue, ieQueue)

//    streams(0).print(Printed.toSysOut())
//    streams(1).print(Printed.toSysOut())
//    streams(2).print(Printed.toSysOut())
    streams(0).to("test-topic-profileus1", Produced.`with`(Serdes.String(), serde))
    streams(1).to("test-topic-profilegb1", Produced.`with`(Serdes.String(), serde))
    streams(2).to("test-topic-profileie1", Produced.`with`(Serdes.String(), serde))

    val kEventStream = new KafkaStreams(streamsBuilder.build, props)

    val latch = new CountDownLatch(1)
    // attach shutdown handler to catch control-c
    Runtime.getRuntime.addShutdownHook(new Thread("streams-shutdown-hook") {
      override def run(): Unit = {
        print("Stopping the streams")
        kEventStream.close
        latch.countDown()
      }
    })
    try {
      println("Starting the stream")
      kEventStream.start
      latch.await()
    } catch {
      case e: Throwable =>
        System.exit(1)
    }
    System.exit(0)
  }
}
