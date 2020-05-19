package com.craftcodehouse.promotions.accumulator

import java.util

import com.craftcodehouse.promotions.accumulator.{CustomAvroSerde, CustomerReward}
import com.craftcodehouse.promotions.accumulator.PaymentAccumulator.getClass
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.io.Source

object GetSerdes {

  def getCustomerRewardAvroSerde(configMap: util.HashMap[String, String]): CustomAvroSerde[CustomerReward] = {
    val filename = "/customer_reward.avsc"
    val fileContents = Source.fromURL(getClass.getResource(filename)).mkString
    val parser = new Parser
    val schema = parser.parse(fileContents)

    val converterTo = (avroRecord: GenericData.Record, reward: CustomerReward) => {
      avroRecord.put("customerID", reward.customerID)
    }

    val converterFrom = (record: GenericRecord) => {
      val customerID = record.get("customerID").toString
      CustomerReward(customerID)
    }
    val rewardSerde = new CustomAvroSerde[CustomerReward](schema, converterTo, converterFrom)
    rewardSerde.configure(configMap, false)
    rewardSerde
  }

  def getCustomerAccumulationAvroSerde(configMap: util.HashMap[String, String]): CustomAvroSerde[CustomerAccumulation] = {
    val filename = "/customer_accumulation.avsc"
    val fileContents = Source.fromURL(getClass.getResource(filename)).mkString
    val parser = new Parser
    val schema = parser.parse(fileContents)

    val converterTo = (avroRecord: GenericData.Record, acc: CustomerAccumulation) => {
      avroRecord.put("customerID", acc.customerID)
      avroRecord.put("stakeAccumulation", acc.stakeAccumulation)
    }

    val converterFrom = (record: GenericRecord) => {
      val customerID = record.get("customerID").toString
      val stakeAccumulation = record.get("stakeAccumulation").asInstanceOf[Int]
      CustomerAccumulation(customerID, stakeAccumulation)
    }
    val accSerde = new CustomAvroSerde[CustomerAccumulation](schema, converterTo, converterFrom)
    accSerde.configure(configMap, false)
    accSerde
  }
}



