package com.craftcodehouse.promotions.accumulator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Date

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


class CustomerAccumulationSerializer extends Serializer[CustomerAccumulation] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def serialize(topic: String, data: CustomerAccumulation): Array[Byte] = {
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

class CustomerAccumulationDeserializer extends Deserializer[CustomerAccumulation] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def deserialize(topic: String, data: Array[Byte]): CustomerAccumulation = {
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
    val customer = customerObj.asInstanceOf[CustomerAccumulation]
    //    println("Topic is " + topic + "Customer in deser is " + customer + " time is " + timeStamp)
    customer
  }

  override def close(): Unit = {
  }
}


class CustomerAccumulationSerde extends Serde[CustomerAccumulation] {
  override def serializer(): Serializer[CustomerAccumulation] = new CustomerAccumulationSerializer

  override def deserializer(): Deserializer[CustomerAccumulation] = new CustomerAccumulationDeserializer
}

