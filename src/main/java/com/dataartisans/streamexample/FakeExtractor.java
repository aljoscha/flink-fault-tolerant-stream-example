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
package com.dataartisans.streamexample;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class FakeExtractor implements FlatMapFunction<String, Edit> {

	@Override
	public void flatMap(String input, Collector<Edit> out) throws Exception {
		String[] split = input.split(",");
		if (split.length != 2) {
			return;
		}

		String channel = split[0];
		String user = split[1];
		Edit edit = new Edit();
		edit.setChannel(channel);
		edit.setUser(user);
		out.collect(edit);
	}

}
