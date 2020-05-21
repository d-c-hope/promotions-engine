package com.craftcodehouse.promotions.accumulator

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.annotation.InterfaceStability
import org.apache.kafka.common.serialization.Deserializer
import java.util

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

import scala.io.Source

class CustomAvroDeserializer[T](schema: Schema,
                             converter: (GenericRecord)=>T)
  extends Deserializer[T] {

  var inner = new KafkaAvroDeserializer

  override def configure(deserializerConfig: util.Map[String, _], isDeserializerForRecordKeys: Boolean): Unit = {
    inner.configure(deserializerConfig, isDeserializerForRecordKeys)
  }

  override def deserialize(topic: String, bytes: Array[Byte]): T = {
    val record = inner.deserialize(topic, bytes, schema).asInstanceOf[GenericRecord]
    converter(record)
  }

  override def close(): Unit = {
    inner.close()
  }
}
