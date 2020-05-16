package com.craftcodehouse.promotions.accumulator

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream._
import java.util
import java.util.{Collections, Date, Locale, Properties}
import java.util.concurrent.CountDownLatch

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde


object BetsAccumulator {

  def setupGameEventStream(props: Properties, streamsBuilder: StreamsBuilder,
                           avroSerde: GenericAvroSerde): KStream[String, GameEvent] = {

//    val gameEventStreamsBuilder = new StreamsBuilder
    val gameEventAvroStream = streamsBuilder.stream("test-topic-game1",
      Consumed.`with`(Serdes.String(), avroSerde))
    val gameEventStream: KStream[String, GameEvent] = gameEventAvroStream.mapValues { value =>
      println("Processing game event")
      val customerID = value.get("customerID").toString
      val game = value.get("game").toString
      val action = value.get("action").toString
      val stake = value.get("stake").asInstanceOf[Int]
      val gameEvent = GameEvent(game, action, customerID, stake)

      gameEvent
    }
//    gameEventStream.print(Printed.toSysOut())
    return gameEventStream
//    val kGameEventStream = new KafkaStreams(streamsBuilder.build, props)
//    return kGameEventStream
  }

  def setupCustomerStream(props: Properties, streamsBuilder: StreamsBuilder,
          avroSerde: GenericAvroSerde): KTable[String, Customer] = {

//    val customerStreamsBuilder = new StreamsBuilder
    val customerAvroStream = streamsBuilder.stream("test-topic-customer1",
      Consumed.`with`(Serdes.String(), avroSerde))
    val customerStream: KStream[String, Customer] = customerAvroStream.mapValues { value =>
      println("Processing customer")
      val customerID = value.get("customerID").toString
      val email = value.get("email").toString
      val firstName = value.get("firstName").toString
      val customer = Customer(customerID, email, firstName)

      customer
    }

    val gr: Grouped[String, Customer] = Grouped.`with`(Serdes.String(), new CustomerSerde)
    val customerTable = customerStream.groupByKey(gr).reduce { (aggValue, newValue) =>
      newValue
    }

//    customerTable.toStream().print(Printed.toSysOut())
    return customerTable
//    new KafkaStreams(customerStreamsBuilder.build, props)

  }

  def main(args: Array[String]): Unit = {

    println("\n*******************\nRunning the app\n\n\n")
//    System.exit(0)

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-promotionsx23254")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Integer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put("schema.registry.url", "http://my-schema-registry:8081")

    val avroSerde = new GenericAvroSerde
    val jmap = new java.util.HashMap[String, String]()
    jmap.put("schema.registry.url", "http://localhost:8081")
    avroSerde.configure(jmap, false)

    val streamsBuilder = new StreamsBuilder
    val customerTable: KTable[String, Customer]  = setupCustomerStream(props, streamsBuilder, avroSerde)
    val gameEventStream = setupGameEventStream(props, streamsBuilder, avroSerde)

    val valueJoiner: ValueJoiner[GameEvent, Customer, String] = (gameEvent:GameEvent, customer: Customer) => {
      String.format("Number: %d", 34);
    }

    val joinedStream = gameEventStream.leftJoin(customerTable, valueJoiner,
      Joined.`with`(Serdes.String(), new GameEventSerde, new CustomerSerde))

    joinedStream.print(Printed.toSysOut())

//    val kCustomerStream = setupCustomerStream(props, avroSerde)
//    val kGameEventStream = setupGameEventStream(props, avroSerde)
    val kEventStream = new KafkaStreams(streamsBuilder.build, props)
//    print(kGameEventStream)

    val latch = new CountDownLatch(1)
    // attach shutdown handler to catch control-c
    Runtime.getRuntime.addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
      override def run(): Unit = {
        print("Stopping the streams")
        kEventStream.close
//        kGameEventStream.close
//        kCustomerStream.close
        latch.countDown()
      }
    })
    try {
      println("Starting the stream")
      kEventStream.start
//      kGameEventStream.start
//      kCustomerStream.start
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




// note easier to make a table of persons by doing mapvalues to the table direct from the stream
//    val customerTable: KTable[String, Customer] = customerStream.groupByKey().reduce((aggValue, newValue) => aggValue)
//    val customerAvroTable: KTable[String, GenericRecord] = customerStreamsBuilder.table("test-topic-customer1", Consumed.`with`(Serdes.String(), avroSerde))
//    customerAvroTable.toStream().print(Printed.toSysOut())

//    val personStream: KStream[String, Person] = personAvroStream.mapValues {
//      personA => Person("111", "andy@mailinator.com", "and")
//    }
//
//    customerTable.toStream().print(Printed.toSysOut())
//    customerTable.p


//    val customerStreamI = new KafkaStreams(customerStreamsBuilder.build, props)
//    val eventStream = new KafkaStreams(streamsBuilder.build, props)

//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde)
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde)
