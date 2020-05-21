package com.craftcodehouse.promotions.accumulator

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.annotation.InterfaceStability
import org.apache.kafka.common.serialization.Serializer
import java.util

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema

class CustomAvroSerializer[T](schema: Schema,
                              converter: (GenericData.Record,T)=>Unit)
  extends Serializer[T] {

  var inner = new KafkaAvroSerializer

  override def configure(serializerConfig: util.Map[String, _], isSerializerForRecordKeys: Boolean): Unit = {
    inner.configure(serializerConfig, isSerializerForRecordKeys)
  }

  override def serialize(topic: String, customerReward: T): Array[Byte] = {

    val avroRecord = new GenericData.Record(schema)
    converter(avroRecord, customerReward)
    inner.serialize(topic, avroRecord)
  }

  override def close(): Unit = {
    inner.close()
  }
}