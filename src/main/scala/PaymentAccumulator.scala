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
import org.apache.avro.generic.GenericRecord
import java.util
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

case class Customer(customerID: String, email: String, firstName: String)
case class GameStake(game: String, action: String, customerId: String, stake: Int)

class CustomerSerializer extends Serializer[Customer] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def serialize(topic: String, data: Customer): Array[Byte] = {
    val date = new Date()
    val timeStamp = date.getTime

//    println("Topic is " + topic + " Customer before ser is " + data + " time is " + timeStamp)
    val out = new ByteArrayOutputStream
    val os = new ObjectOutputStream(out)
    os.writeObject(data)
    os.flush()
    out.toByteArray
  }

  override def close(): Unit = {
  }
}

class CustomerDeserializer extends Deserializer[Customer] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def deserialize(topic: String, data: Array[Byte]): Customer = {
    val date = new Date()
    val timeStamp = date.getTime

    val in = new ByteArrayInputStream(data)
    val is = new ObjectInputStream(in)
    val customerObj = is.readObject
    if (in != null) {
      in.close()
    }
    if (is != null) {
      is.close()
    }
    val customer = customerObj.asInstanceOf[Customer]
//    println("Topic is " + topic + "Customer in deser is " + customer + " time is " + timeStamp)
    customer
  }

  override def close(): Unit = {
  }
}


class CustomerSerde extends Serde[Customer] {
  override def serializer(): Serializer[Customer] = new CustomerSerializer

  override def deserializer(): Deserializer[Customer] = new CustomerDeserializer
}

object BetsAccumulator {



  def main(args: Array[String]): Unit = {

    println("\n*******************\nRunning the app\n\n\n")
//    System.exit(0)

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-promotions")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.asInstanceOf[Integer])
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde)
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde)
    props.put("schema.registry.url", "http://my-schema-registry:8081")


    val avroSerde = new GenericAvroSerde
    val jmap = new java.util.HashMap[String, String]()
    jmap.put("schema.registry.url", "http://localhost:8081")
    avroSerde.configure(jmap, false)

    val eventStreamsBuilder = new StreamsBuilder
    val eventAvroStream = eventStreamsBuilder.stream("test-topic-game-1", Consumed.`with`(avroSerde, avroSerde))

    val customerStreamsBuilder = new StreamsBuilder
    val customerAvroStream = customerStreamsBuilder.stream("test-topic-customer1", Consumed.`with`(Serdes.String(), avroSerde))
    val customerStream: KStream[String, Customer] = customerAvroStream.mapValues { value =>
          val customerID = value.get("customerID").toString
          val email = value.get("email").toString
          val firstName = value.get("firstName").toString
          val customer = Customer(customerID, email, firstName)
//          println(customer)
          customer
    }
//    customerStream.print(Printed.toSysOut())

    val gr: Grouped[String, Customer] = Grouped.`with`(Serdes.String(), new CustomerSerde)
    val customerTable = customerStream.groupByKey(gr).reduce { (aggValue, newValue) =>
//      println("New value is " + newValue)
      newValue
    }


//      (aggValue, newValue) => aggValue)
    customerTable.toStream().print(Printed.toSysOut())


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

    val kCustomerStream = new KafkaStreams(customerStreamsBuilder.build, props)
//    val customerStreamI = new KafkaStreams(customerStreamsBuilder.build, props)
//    val eventStream = new KafkaStreams(streamsBuilder.build, props)

    val latch = new CountDownLatch(1)
    // attach shutdown handler to catch control-c
    Runtime.getRuntime.addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
      override def run(): Unit = {
//        streams.close
//        customerStreamI.close
        kCustomerStream.close
        latch.countDown()
      }
    })
    try {
//      streams.start
      println("Starting the stream")
//      customerStreamI.start
      kCustomerStream.start
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



