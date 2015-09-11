package com.dataartisans.streamexample;

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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Job {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		Properties props = new Properties();
		props.setProperty("zookeeper.connect", "localhost:2181");
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "fault-tolerance-example");
		props.setProperty("auto.commit.enable", "false");
		props.setProperty("auto.offset.reset", "largest");

		DataStream<String> kafkaStream = env
				.addSource(new FlinkKafkaConsumer<>("wikipedia-raw",
						new MySimpleStringSchema(),
						props,
						FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
						FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));

		DataStream<Edit> parsedStream = kafkaStream.flatMap(new JsonExtractor());

		parsedStream.print();

		env.execute("Fault-Tolerant Stream Example");
	}
}
