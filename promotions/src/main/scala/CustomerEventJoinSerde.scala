package com.craftcodehouse.promotions.accumulator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Date

import com.craftcodehouse.promotions.accumulator.CustomerEventJoin
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


class CustomerEventJoinSerializer extends Serializer[CustomerEventJoin] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def serialize(topic: String, data: CustomerEventJoin): Array[Byte] = {
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

class CustomerEventJoinDeserializer extends Deserializer[CustomerEventJoin] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def deserialize(topic: String, data: Array[Byte]): CustomerEventJoin = {
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
    val customer = customerObj.asInstanceOf[CustomerEventJoin]
    //    println("Topic is " + topic + "Customer in deser is " + customer + " time is " + timeStamp)
    customer
  }

  override def close(): Unit = {
  }
}


class CustomerEventJoinSerde extends Serde[CustomerEventJoin] {
  override def serializer(): Serializer[CustomerEventJoin] = new CustomerEventJoinSerializer

  override def deserializer(): Deserializer[CustomerEventJoin] = new CustomerEventJoinDeserializer
}

