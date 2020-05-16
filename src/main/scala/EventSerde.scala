package com.craftcodehouse.promotions.accumulator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Date

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


class GameEventSerializer extends Serializer[GameEvent] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def serialize(topic: String, data: GameEvent): Array[Byte] = {
    val date = new Date()
    val timeStamp = date.getTime

    val out = new ByteArrayOutputStream
    val os = new ObjectOutputStream(out)
    os.writeObject(data)
    os.flush()
    out.toByteArray
  }

  override def close(): Unit = {
  }
}

class GameEventDeserializer extends Deserializer[GameEvent] {
  def configure(map: Nothing, b: Boolean): Unit = {
  }

  def deserialize(topic: String, data: Array[Byte]): GameEvent = {
    val date = new Date()
    val timeStamp = date.getTime

    val in = new ByteArrayInputStream(data)
    val is = new ObjectInputStream(in)
    val gameEventObj = is.readObject
    if (in != null) {
      in.close()
    }
    if (is != null) {
      is.close()
    }
    val gameEvent = gameEventObj.asInstanceOf[GameEvent]
    gameEvent
  }

  override def close(): Unit = {
  }
}


class GameEventSerde extends Serde[GameEvent] {
  override def serializer(): Serializer[GameEvent] = new GameEventSerializer
  override def deserializer(): Deserializer[GameEvent] = new GameEventDeserializer
}
