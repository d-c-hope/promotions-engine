package com.craftcodehouse.ims.serdes


import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.util

import com.craftcodehouse.ims.Profile_created
import com.fasterxml.jackson.core.`type`.TypeReference

import scala.reflect.ClassTag
import scala.reflect._



class JsonDeserializer[T](implicit m: scala.reflect.ClassTag[T]) extends Deserializer[T] {

  private val objectMapper = new ObjectMapper
  objectMapper.registerModule(DefaultScalaModule)

//  implicit val m: scala.reflect.ClassTag[T]

//  def myClassOf[E:ClassTag] = implicitly[ClassTag[E]].runtimeClass
//  val ct = classTag[T]

  override def configure(props: util.Map[String, _], isKey: Boolean): Unit = {
  }


  //  obj
  override def deserialize(topic: String, bytes: Array[Byte]): T = {

    val data = objectMapper.readValue(bytes, m.runtimeClass).asInstanceOf[T]
    return data
  }


//  def fromJson[V](bytes: Array[Byte])(implicit m: scala.reflect.ClassTag[V]): V = {
//
//    objectMapper.readValue(bytes, m.runtimeClass)
//  }



  override def close(): Unit = {
  }
}


//  def fromJson[T](bytes: Array[Byte])(implicit m : Manifest[T]): T = {



//package com.craftcodehouse.ims.serdes
//
//
//import com.fasterxml.jackson.databind.ObjectMapper
//import org.apache.kafka.common.errors.SerializationException
//import org.apache.kafka.common.serialization.Deserializer
//import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import java.util
//
//import com.craftcodehouse.ims.Profile_created
//
//import scala.reflect.ClassTag
//
//
//
//class JsonDeserializer extends Deserializer[Profile_created] {
//
//  private val objectMapper = new ObjectMapper
//  objectMapper.registerModule(DefaultScalaModule)
////  private var tClass = null
////  implicit m: Manifest[T]
//
////  def myClassOf[E:ClassTag] = implicitly[ClassTag[E]].runtimeClass
//
//  override def configure(props: util.Map[String, _], isKey: Boolean): Unit = {
//  }
//
////  obj
//  override def deserialize(topic: String, bytes: Array[Byte]): Profile_created = {
////    if (bytes == null) return null
////    lazy val mapper = new ObjectMapper() with ScalaObjectMapper
////    objectMapper.registerModule(DefaultScalaModule)
//    println("deserializing")
//    val data = objectMapper.readValue(bytes, classOf[Profile_created])
//    println(data)
////    objectMapper.readV
//    return data
////    objectMapper.read
//
////    var data = null
////    try data = objectMapper.readValue(bytes, Class.forName(className))
////    catch {
////      case e: Exception =>
////        throw new SerializationException(e)
////    }
////    data
//  }
//
//  override def close(): Unit = {
//  }
//}
