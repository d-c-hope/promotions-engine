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
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
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

    val gr: Grouped[String, Customer] = Grouped.`with`(Serdes.String(), new GenericCaseClassSerde[Customer])
    val customerTable = customerStream.groupByKey(gr).reduce { (aggValue, newValue) =>
      newValue
    }

    return customerTable
  }

  def setupCustomerEventJoiner(gameEventStream: KStream[String, GameEvent],
                               customerTable: KTable[String, Customer]) : KStream[String, CustomerEventJoin] = {

    val valueJoiner: ValueJoiner[GameEvent, Customer, CustomerEventJoin] = (gameEvent:GameEvent, customer: Customer) => {
//      print("Customer is " + customer + "game event is " + gameEvent, "customer id is " + gameEvent.customerId)
      CustomerEventJoin(customer.customerID, customer.email, customer.firstName,
        gameEvent.game, gameEvent.action, gameEvent.stake)
    }


    val joinedStream = gameEventStream.leftJoin(customerTable, valueJoiner,
      Joined.`with`(Serdes.String(), new GameEventSerde, new GenericCaseClassSerde[Customer]))

    //      // NOT NEEDED
//    val passAll: Predicate[String, CustomerEventJoin] = (key: String, event: CustomerEventJoin) => true
//    val splitKStreamList = joinedStream.branch(passAll, passAll)

    joinedStream

  }

  def setupAccumlationTable(joinedStream: KStream[String, CustomerEventJoin]):KTable[String, CustomerAccumulation] = {

    val gr: Grouped[String, CustomerEventJoin] = Grouped.`with`(Serdes.String(), new CustomerEventJoinSerde)

    val aggInit: Initializer[CustomerAccumulation] = () => CustomerAccumulation("", 0)
    val agg: Aggregator[String, CustomerEventJoin, CustomerAccumulation] =
      (key: String, value: CustomerEventJoin, aggregate: CustomerAccumulation) => {
      CustomerAccumulation(value.customerID, aggregate.stakeAccumulation + value.stake)
    }

    val accTable = joinedStream.groupByKey(gr).aggregate(aggInit, agg, Materialized.`with`(Serdes.String(), new CustomerAccumulationSerde))
//    accTable.toStream().print(Printed.toSysOut())
//    val accStream = accTable.toStream()
    return accTable

  }

  def setupRewardsStream(joinedStream: KStream[String, CustomerEventJoin]): KStream[String, CustomerReward] = {
    println("Setting up rewards stream")
    val transformerSupplier: ValueTransformerSupplier[CustomerEventJoin, CustomerReward] = () => {
      new ValueTransformer[CustomerEventJoin, CustomerReward]() {

        var context: ProcessorContext = null
        var store: KeyValueStore[String, Int] = null

        override def init(context: ProcessorContext): Unit = {
          println("Setting up transform processor")
          store = context.getStateStore("stakeAccumulationStore").asInstanceOf[KeyValueStore[String, Int]]
        }

        override def transform(value: CustomerEventJoin): CustomerReward = {
          // this bit needs the logic to get the value from teh store, add the latest even
          // and either emit null or a reward event
//          println("In transform*********************************************\n\n")
          val rewardAccumulated = store.get(value.customerID)
          val eventStake = value.stake
          if (rewardAccumulated + eventStake > 1000) {
            store.put(value.customerID, 0)
            return CustomerReward(value.customerID)
          }
          else {
            store.put(value.customerID, rewardAccumulated + eventStake)
            return null;
          }

        }

        override def close(): Unit = {

        }
      }
    }

    val transformedStream = joinedStream.transformValues(transformerSupplier, "stakeAccumulationStore")

    val passNoneNull: Predicate[String, CustomerReward] = (key: String, value: CustomerReward) => {
      if(value != null) true else false
    }
    transformedStream.filter(passNoneNull)

  }

//  def handleAccumulationReset = {
  // See above now, nearly done
//    // create store, need to do this further up

    // See transform values. Need to do something like get value out of the store add the latest value
    // see if it is bigger than the threshold, if it is then we emit a value
    // Alternative to using the DSL and effectively adding 2 fields to accumulation (alongside absolute total)
    // We will check if over threshold. If so reset to zero and set flag saying points can be added
    // Then on subsequent message if flat is set we reset it
    // Then we shall have a filter  that looks for the flag set.
    // Problem is, the table might not get pushed downsstream befoe the update where the flag goes off
    // Maybe this can't work

//  }

  def main(args: Array[String]): Unit = {

    println("\n*******************\nRunning the app\n\n\n")
//    System.exit(0)

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-promotionsx237593")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Integer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put("schema.registry.url", "http://my-schema-registry:8081")

    val avroSerde = new GenericAvroSerde
    val jmap = new java.util.HashMap[String, String]()
    jmap.put("schema.registry.url", "http://localhost:8081")
    avroSerde.configure(jmap, false)

    val streamsBuilder = new StreamsBuilder

    val storeSupplier: KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore("stakeAccumulationStore");
    val storeBuilder: StoreBuilder[KeyValueStore[String, Integer]]  =
      Stores.keyValueStoreBuilder(storeSupplier,
        Serdes.String(),
        Serdes.Integer());
    streamsBuilder.addStateStore(storeBuilder)

    val customerTable: KTable[String, Customer]  = setupCustomerStream(props, streamsBuilder, avroSerde)
    val gameEventStream = setupGameEventStream(props, streamsBuilder, avroSerde)
    val joinedStream = setupCustomerEventJoiner(gameEventStream, customerTable)

//    val kStreamForAccTable = branchedStream(0)
//    val kStreamForRewards = branchedStream(1)
//    println("\n\n K stream for rewards" + kStreamForRewards)
    val accTable = setupAccumlationTable(joinedStream)
    val rewardsStream = setupRewardsStream(joinedStream)
    // TODO work out why the rewards stream is empty - answer is we don't need to branch if we want a copy
    // branching is only for when you want to split the stream up
    rewardsStream.print(Printed.toSysOut())
    accTable.toStream().print(Printed.toSysOut())
//    kStreamForRewards.print(Printed.toSysOut())
//    kStreamEnd.to("test-topic-aggs1")


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




//    class AccValueTransformer extends ValueTransformer[CustomerEventJoin, CustomerAccumulation] {
//
//      override def init(context: ProcessorContext): Unit = {
//        stateStore = context.getStateStore("sfgfdgdf")
//      }
//
//      override def transform(value: CustomerEventJoin): CustomerAccumulation = {
//          return CustomerAccumulation("3454343", 45)
//      }
//
//      override def close(): Unit = {
//
//      }
//    }
//    new ValueTransformer[](context) {}

//    kStreamEnd.print(Printed.toSysOut())