package com.craftcodehouse.ims

import com.craftcodehouse.ims.serdes

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Produced, _}
import java.util.Properties
import java.util.concurrent.CountDownLatch

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, StoreSupplier, Stores}


object IMS {

  def main(args: Array[String]): Unit = {

    println("\n*******************\nRunning the app\n\n\n")

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-imsx237593")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Integer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val streamsBuilder = new StreamsBuilder

    val profileCreatedStream = streamsBuilder.stream("test-topic-profilecreated1",
      Consumed.`with`(Serdes.String(), De))


    rewardsStream.to("test-topic-rewards1", Produced.`with`(Serdes.String(), rewardSerde))

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



//def setupCustomerEventJoiner(gameEventStream: KStream[String, GameEvent],
//customerTable: KTable[String, Customer]) : KStream[String, CustomerEventJoin] = {
//
//  val valueJoiner: ValueJoiner[GameEvent, Customer, CustomerEventJoin] = (gameEvent:GameEvent, customer: Customer) => {
//  CustomerEventJoin(customer.customerID, customer.email, customer.firstName,
//  gameEvent.game, gameEvent.action, gameEvent.stake)
//}
//
//
//  val joinedStream = gameEventStream.leftJoin(customerTable, valueJoiner,
//  Joined.`with`(Serdes.String(), new GameEventSerde, new GenericCaseClassSerde[Customer]))
//
//  joinedStream
//
//}


//def setupProfileStream(props: Properties, streamsBuilder: StreamsBuilder,
//avroSerde: GenericAvroSerde): KStream[String, GameEvent] = {
//
//  Serdes.serdeFrom(iserializer, ideserializer)
//
//  val gameEventAvroStream = streamsBuilder.stream("test-topic-profilecreated1",
//  Consumed.`with`(Serdes.String(), De))
//  val gameEventStream: KStream[String, GameEvent] = gameEventAvroStream.mapValues { value =>
//  //      println("Processing game event")
//  val customerID = value.get("customerID").toString
//  val game = value.get("game").toString
//  val action = value.get("action").toString
//  val stake = value.get("stake").asInstanceOf[Int]
//  val gameEvent = GameEvent(game, action, customerID, stake)
//
//  gameEvent
//}
//
//  return gameEventStream
//}