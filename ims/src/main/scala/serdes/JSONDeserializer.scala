package com.craftcodehouse.ims.serdes


import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import java.util



class JsonPOJODeserializer[T]() extends Deserializer[T] {

  private val objectMapper = new ObjectMapper
  private var tClass = null

  override def configure(props: util.Map[String, _], isKey: Boolean): Unit = {
    tClass = props.get("JsonPOJOClass").asInstanceOf[Class[T]]
  }

  override def deserialize(topic: String, bytes: Array[Byte]): T = {
    if (bytes == null) return null
    var data = null
    try data = objectMapper.readValue(bytes, tClass)
    catch {
      case e: Exception =>
        throw new SerializationException(e)
    }
    data
  }

  override def close(): Unit = {
  }
}
