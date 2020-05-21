package com.craftcodehouse.ims.serdes


import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.util

import scala.reflect.ClassTag
import scala.reflect._



class JsonDeserializer[T](implicit m: scala.reflect.ClassTag[T]) extends Deserializer[T] {

  private val objectMapper = new ObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

  override def configure(props: util.Map[String, _], isKey: Boolean): Unit = {
  }

  override def deserialize(topic: String, bytes: Array[Byte]): T = {
    val data = objectMapper.readValue(bytes, m.runtimeClass).asInstanceOf[T]
    return data
  }

  override def close(): Unit = {
  }
}
