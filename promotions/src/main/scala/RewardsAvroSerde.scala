//package com.craftcodehouse.promotions.accumulator
//
//import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
//import java.util.Date
//
//import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
//
//
//class RewardsAvroSerializer extends Serializer[CustomerReward] {
//  def configure(map: Nothing, b: Boolean): Unit = {
//  }
//
//  def serialize(topic: String, data: CustomerReward): Array[Byte] = {
//    import org.apache.avro.generic.GenericData
//    import org.apache.avro.generic.GenericRecord
////    val key = "key1"
//    val rewardSchema = "{\"type\":\"record\"," + "\"name\":\"customerrewardv01\"," +
//      "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}"
//    val parser = new Nothing
//    val schema = parser.parse(rewardSchema)
//    val avroRecord = new GenericData.Record(schema)
//    avroRecord.put("f1", "value1")
////    avroRecord.
//
//    //    val date = new Date()
////    val timeStamp = date.getTime
////
////    val out = new ByteArrayOutputStream
////    val os = new ObjectOutputStream(out)
////    os.writeObject(data)
////    os.flush()
////    out.toByteArray
//  }
//
//  override def close(): Unit = {
//  }
//}
//
//class RewardsAvroDeserializer extends Deserializer[CustomerReward] {
//  def configure(map: Nothing, b: Boolean): Unit = {
//  }
//
//  def deserialize(topic: String, data: Array[Byte]): CustomerReward = {
////    val date = new Date()
////    val timeStamp = date.getTime
////
////    val in = new ByteArrayInputStream(data)
////    val is = new ObjectInputStream(in)
////    val gameEventObj = is.readObject
////    if (in != null) {
////      in.close()
////    }
////    if (is != null) {
////      is.close()
////    }
////    val gameEvent = gameEventObj.asInstanceOf[GameEvent]
////    gameEvent
//  }
//
//  override def close(): Unit = {
//  }
//}
//
//
//class RewardsAvroSerde extends Serde[CustomerReward] {
//  override def serializer(): Serializer[CustomerReward] = new RewardsAvroSerializer
//  override def deserializer(): Deserializer[CustomerReward] = new RewardsAvroDeserializer
//}
