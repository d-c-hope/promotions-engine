package com.craftcodehouse.promotions.accumulator

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream._
import java.util.Properties
import java.util.concurrent.CountDownLatch

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, StoreSupplier, Stores}


object BetsAccumulator {

  def setupGameEventStream(props: Properties, streamsBuilder: StreamsBuilder,
                           avroSerde: GenericAvroSerde): KStream[String, GameEvent] = {

    val gameEventAvroStream = streamsBuilder.stream("test-topic-game1",
      Consumed.`with`(Serdes.String(), avroSerde))
    val gameEventStream: KStream[String, GameEvent] = gameEventAvroStream.mapValues { value =>
//      println("Processing game event")
      val customerID = value.get("customerID").toString
      val game = value.get("game").toString
      val action = value.get("action").toString
      val stake = value.get("stake").asInstanceOf[Int]
      val gameEvent = GameEvent(game, action, customerID, stake)

      gameEvent
    }

    return gameEventStream
  }

  def setupCustomerStream(props: Properties, streamsBuilder: StreamsBuilder,
          avroSerde: GenericAvroSerde): KTable[String, Customer] = {

    val customerAvroStream = streamsBuilder.stream("test-topic-customer1",
      Consumed.`with`(Serdes.String(), avroSerde))
    val customerStream: KStream[String, Customer] = customerAvroStream.mapValues { value =>
//      println("Processing customer")
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

    return customerTable
  }

  def setupCustomerEventJoiner(gameEventStream: KStream[String, GameEvent],
                               customerTable: KTable[String, Customer]) : Unit = {

    val valueJoiner: ValueJoiner[GameEvent, Customer, CustomerEventJoin] = (gameEvent:GameEvent, customer: Customer) => {
//      print("Customer is " + customer + "game event is " + gameEvent, "customer id is " + gameEvent.customerId)
      CustomerEventJoin(customer.customerID, customer.email, customer.firstName,
        gameEvent.game, gameEvent.action, gameEvent.stake)
    }

    val joinedStream = gameEventStream.leftJoin(customerTable, valueJoiner,
      Joined.`with`(Serdes.String(), new GameEventSerde, new CustomerSerde))

    val gr: Grouped[String, CustomerEventJoin] = Grouped.`with`(Serdes.String(), new CustomerEventJoinSerde)

    //need to replace this with a transform values
    val aggInit: Initializer[CustomerAccumulation] = () => CustomerAccumulation("", 0)
    val agg: Aggregator[String, CustomerEventJoin, CustomerAccumulation] =
      (key: String, value: CustomerEventJoin, aggregate: CustomerAccumulation) => {
      CustomerAccumulation(value.customerID, aggregate.stakeAccumulation + value.stake)
    }

    val accTable = joinedStream.groupByKey(gr).aggregate(aggInit, agg, Materialized.`with`(Serdes.String(), new CustomerAccumulationSerde))
    accTable.toStream().print(Printed.toSysOut())
    val accStream = accTable.toStream()

//    val x = new Predicate[String, CustomerAccumulation]((key: String, event: CustomerAccumulation) => true)
    val passAll: Predicate[String, CustomerAccumulation] = (key: String, event: CustomerAccumulation) => true

//    val passAll = new Predicate[String, CustomerAccumulation] {
//      override def test(key: String, value: CustomerAccumulation): Boolean = true
//    }


    val passOverX: Predicate[String, CustomerAccumulation] = (key: String, event: CustomerAccumulation) => {
      if (event.stakeAccumulation > 2000) true else false
    }
    val splitKStreamList = accStream.branch(passAll, passOverX)
    val kStreamEnd = splitKStreamList(0)
    kStreamEnd.to("test-topic-aggs1")
    val kStreamForAgg = splitKStreamList(1)

    //    val customerTable = customerStream.groupByKey(gr).reduce { (aggValue, newValue) =>
//      newValue
//    }
//
    kStreamEnd.print(Printed.toSysOut())

  }

  def handleAccumulationReset = {
//    // create store, need to do this further up

    // See transform values. Need to do something like get value out of the store add the latest value
    // see if it is bigger than the threshold, if it is then we emit a value
    // Alternative to using the DSL and effectively adding 2 fields to accumulation (alongside absolute total)
    // We will check if over threshold. If so reset to zero and set flag saying points can be added
    // Then on subsequent message if flat is set we reset it
    // Then we shall have a filter  that looks for the flag set.
    // Problem is, the table might not get pushed downsstream befoe the update where the flag goes off
    // Maybe this can't work

  }

  def main(args: Array[String]): Unit = {

    println("\n*******************\nRunning the app\n\n\n")
//    System.exit(0)

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-promotionsx23754")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Integer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put("schema.registry.url", "http://my-schema-registry:8081")

    val avroSerde = new GenericAvroSerde
    val jmap = new java.util.HashMap[String, String]()
    jmap.put("schema.registry.url", "http://localhost:8081")
    avroSerde.configure(jmap, false)

    val streamsBuilder = new StreamsBuilder

//    val stakeAccumulationStore: KeyValueStore[String, Int]
    val storeSupplier: KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore("stakeAccumulationStore");         1
    val storeBuilder: StoreBuilder[KeyValueStore[String, Integer]]  =
      Stores.keyValueStoreBuilder(storeSupplier,
        Serdes.String(),
        Serdes.Integer());
    streamsBuilder.addStateStore(storeBuilder)

    val customerTable: KTable[String, Customer]  = setupCustomerStream(props, streamsBuilder, avroSerde)
    val gameEventStream = setupGameEventStream(props, streamsBuilder, avroSerde)
    setupCustomerEventJoiner(gameEventStream, customerTable)

    val kEventStream = new KafkaStreams(streamsBuilder.build, props)

    val latch = new CountDownLatch(1)
    // attach shutdown handler to catch control-c
    Runtime.getRuntime.addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
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
