package com.craftcodehouse.ims.serdes


import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import java.util

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


class JsonPOJODeserializer[T]()

/**
 * Default constructor needed by Kafka
 */
  extends Deserializer[T] {
  private val objectMapper = new ObjectMapper
  private var tClass = null

  @SuppressWarnings(Array("unchecked")) override def configure(props: util.Map[String, _], isKey: Boolean): Unit = {
    tClass = props.get("JsonPOJOClass").asInstanceOf[Class[T]]
  }

  override def deserialize(topic: String, bytes: Array[Byte]): T = {
    if (bytes == null) return null
    var data = null
    try data = objectMapper.readValue(bytes, tClass)
    catch {
      case e: Exception =>
        throw new SerializationException(e)
    }
    data
  }

  override def close(): Unit = {
  }
}
