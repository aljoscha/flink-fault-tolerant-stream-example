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
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.checkpoint.Checkpointed
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.windowing.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer.{FetcherType, OffsetStore}
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource
import org.apache.flink.streaming.util.serialization.{SerializationSchema, DeserializationSchema}

object ScalaJobCheatSheet {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(500)
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

    val result = parsedStream
      .map { edit => (edit.getChannel, edit.getUser, 1) }
      .groupBy(1)
      .window(Time.of(5, TimeUnit.SECONDS))
      .sum(2)
      .flatten()
//      .filter( _._3 > 2 )
      .map(new StatefulCounter)

    result.print()

    result.addSink(new KafkaSink("localhost:9092", "flink-output", new MySimpleStringSchema))


    env.execute("Fault-Tolerant Scala Stream Example")
  }
}


class StatefulCounter extends RichMapFunction[(String, String, Int), String] with Checkpointed[Integer] {
  private var count: Int = 0

  def map(in: (String, String, Int)): String = {
    count += in._3
    "Count: " + count
  }

  def snapshotState(l: Long, l1: Long): Integer = {
    count
  }

  def restoreState(state: Integer) {
    count = state
  }
}

class CounterCounter extends RichMapFunction[(String, String, Int), String] {
  private var count: Int = 0

  def map(in: (String, String, Int)): String = {
    count += in._3
    "Count: " + count
  }
}





