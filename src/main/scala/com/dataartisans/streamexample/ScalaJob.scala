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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.{FetcherType, OffsetStore}

object ScalaJob {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.enableCheckpointing(500)
    env.setParallelism(1)

    val props = new Properties
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "fault-tolerance-example")
    props.setProperty("auto.commit.enable", "false")
    props.setProperty("auto.offset.reset", "largest")

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String](
      "wikipedia-raw",
      new MySimpleStringSchema,
      props,
      OffsetStore.FLINK_ZOOKEEPER,
      FetcherType.LEGACY_LOW_LEVEL))

    val parsedStream = kafkaStream.flatMap(new JsonExtractor)

    parsedStream.print()

    env.execute("Fault-Tolerant Scala Stream Example")
  }
}



