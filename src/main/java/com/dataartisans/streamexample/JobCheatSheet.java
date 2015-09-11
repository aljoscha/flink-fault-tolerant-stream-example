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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JobCheatSheet {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500);
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

		DataStream<String> result = parsedStream
				.map( edit -> new Tuple3<>(edit.getChannel(), edit.getUser(), 1) ).returns("Tuple3<String,String,Integer>")
				.groupBy(1)
				.window(Time.of(5, TimeUnit.SECONDS))
				.sum(2)
				.flatten()
//				.filter(v -> v.f2 > 2)
				.map(new StatefulCounter());

		result.print();

		result.addSink(new KafkaSink<>("localhost:9092", "flink-output", new MySimpleStringSchema()));



		env.execute("Fault-Tolerant Stream Example");
	}

	public static class StatefulCounter extends RichMapFunction<Tuple3<String, String, Integer>, String> implements Checkpointed<Integer> {
		private static final long serialVersionUID = 1L;

		private int count = 0;

		@Override
		public String map(Tuple3<String, String, Integer> in) throws Exception {
			count += in.f2;
			return "Count: " + count;
		}

		@Override
		public Integer snapshotState(long l, long l1) throws Exception {
			return count;
		}

		@Override
		public void restoreState(Integer state) {
			count = state;
		}
	}
}
