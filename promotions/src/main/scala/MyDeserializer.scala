package com.craftcodehouse.promotions.accumulator

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.annotation.InterfaceStability
import org.apache.kafka.common.serialization.Deserializer
import java.util

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema.Parser

import scala.io.Source

class MyDeserializer() extends Deserializer[CustomerReward] {

  var inner = new KafkaAvroDeserializer

  def this(client: SchemaRegistryClient) {
    this()
    inner = new KafkaAvroDeserializer(client)
  }

  override def configure(deserializerConfig: util.Map[String, _], isDeserializerForRecordKeys: Boolean): Unit = {
    inner.configure(deserializerConfig, isDeserializerForRecordKeys)
  }

  override def deserialize(topic: String, bytes: Array[Byte]): CustomerReward = {
    // reader schema
    val filename = "/customer_reward.avsc"
    val fileContents = Source.fromURL(getClass.getResource(filename)).mkString
    val parser = new Parser
    val schema = parser.parse(fileContents)
    val record = inner.deserialize(topic, bytes, schema).asInstanceOf[GenericRecord]
    val customerID = record.get("customerID").toString
    CustomerReward(customerID)
  }

  override def close(): Unit = {
    inner.close()
  }
}