/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.dataartisans.streamexample


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082
//import org.apache.flink.streaming.api.windowing.time.Time

object KafkaConsumer {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val valueDeserializer = new SimpleStringSchema()
    val props = new java.util.Properties()

    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "my-kafka-test")
    props.setProperty("auto.commit.enable", "false")
    props.setProperty("auto.offset.reset", "largest")

    // create a Kafka Consumer
    val kafkaConsumer: FlinkKafkaConsumer082[String] =
      new FlinkKafkaConsumer082(
        "Topic1",
        valueDeserializer,
        props
      )

    // get input data
    val messageStream: DataStream[String] = env.addSource(kafkaConsumer) // supply typeInfo ?

    // do something with it
    val messages = messageStream.
      map ( s => "Kafka and Flink say: $s" )

    // execute and print result
    messages.print()

    env.execute()
  }
}
