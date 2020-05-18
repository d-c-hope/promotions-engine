package com.craftcodehouse.promotions.accumulator

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.annotation.InterfaceStability
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import java.util

import com.craftcodehouse.promotions.accumulator.CustomerAccumulation
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}

class MyAvroSerde() extends Serde[CustomerReward] {

  var inner: Serde[CustomerReward] = Serdes.serdeFrom(new MySerializer(), new MyDeserializer())

  //  def this(client: SchemaRegistryClient)
//    if (client == null) {
//      throw new IllegalArgumentException("schema registry client must not be null");
//    }
//    inner = Serdes.serdeFrom(new GenericAvroSerializer(client),
//      new GenericAvroDeserializer(client));
//  }

  override def serializer: Serializer[CustomerReward] = inner.serializer
  override def deserializer: Deserializer[CustomerReward] = inner.deserializer

  override def configure(serdeConfig: util.Map[String, _], isSerdeForRecordKeys: Boolean): Unit = {
    inner.serializer.configure(serdeConfig, isSerdeForRecordKeys)
    inner.deserializer.configure(serdeConfig, isSerdeForRecordKeys)
  }

  override def close(): Unit = {
    inner.serializer.close()
    inner.deserializer.close()
  }
}
