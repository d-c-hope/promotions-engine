package com.craftcodehouse.promotions.accumulator

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.annotation.InterfaceStability
import org.apache.kafka.common.serialization.Serializer
import java.util

import com.craftcodehouse.promotions.accumulator.CustomerAccumulation
import com.craftcodehouse.promotions.accumulator.PaymentAccumulator.getClass
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema.Parser

import scala.io.Source

class MySerializer() extends Serializer[CustomerReward] {

  var inner = new KafkaAvroSerializer

  def this(client: SchemaRegistryClient) {
    this()
    inner = new KafkaAvroSerializer(client)
  }

  override def configure(serializerConfig: util.Map[String, _], isSerializerForRecordKeys: Boolean): Unit = {
    inner.configure(serializerConfig, isSerializerForRecordKeys)
  }

  override def serialize(topic: String, customerReward: CustomerReward): Array[Byte] = {
    // writer schema
    val filename = "/customer_reward.avsc"
    val fileContents = Source.fromURL(getClass.getResource(filename)).mkString
    val parser = new Parser
    val schema = parser.parse(fileContents)
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("customerID", customerReward.customerID)
    println("serializing " + customerReward.customerID + "avro record" + avroRecord)
    inner.serialize(topic, avroRecord)
  }

  override def close(): Unit = {
    inner.close()
  }
}