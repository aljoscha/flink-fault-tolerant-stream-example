package com.dataartisans.streamexample

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.dataartisans.streamexample.JobCheatSheet.StatefulCounter
import kafka.consumer.ConsumerConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.windowing.Time
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource
import org.apache.flink.streaming.connectors.kafka.api.{KafkaSource, KafkaSink}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

object ScalaJob {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(500)
    env.setParallelism(1)

    val kafkaStream = env.addSource(new KafkaSource[String]("localhost:2181", "wikipedia-raw", new MySimpleStringSchema))


    val parsedStream = kafkaStream.flatMap(new JsonExtractor)

    parsedStream.print()

    env.execute("Fault-Tolerant Scala Stream Example")
  }
}



