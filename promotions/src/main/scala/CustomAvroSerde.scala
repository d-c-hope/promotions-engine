package com.craftcodehouse.promotions.accumulator

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.annotation.InterfaceStability
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import java.util

import com.craftcodehouse.promotions.accumulator.CustomerAccumulation
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.Schema

//class CustomAvroSerde[T]

class CustomAvroSerde[T](schema: Schema,
                              converterTo: (GenericData.Record,T)=>Unit,
                              converterFrom: (GenericRecord)=>T)

  extends Serde[T] {

  val iserializer = new CustomAvroSerializer[T](schema, converterTo)
  val ideserializer = new CustomAvroDeserializer[T](schema, converterFrom)
  var inner: Serde[T] = Serdes.serdeFrom(iserializer, ideserializer)

  //  def this(client: SchemaRegistryClient)
//    if (client == null) {
//      throw new IllegalArgumentException("schema registry client must not be null");
//    }
//    inner = Serdes.serdeFrom(new GenericAvroSerializer(client),
//      new GenericAvroDeserializer(client));
//  }

  override def serializer: Serializer[T] = inner.serializer
  override def deserializer: Deserializer[T] = inner.deserializer

  override def configure(serdeConfig: util.Map[String, _], isSerdeForRecordKeys: Boolean): Unit = {
    inner.serializer.configure(serdeConfig, isSerdeForRecordKeys)
    inner.deserializer.configure(serdeConfig, isSerdeForRecordKeys)
  }

  override def close(): Unit = {
    inner.serializer.close()
    inner.deserializer.close()
  }
}
