package com.craftcodehouse.promotions.accumulator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Date

import com.craftcodehouse.promotions.accumulator.Customer
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


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

