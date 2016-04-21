/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kappaware.kgen;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.jr.ob.JSON;

public class JsonSerializer<T>  implements Serializer<T> {
	private JSON json;
	
	public JsonSerializer(boolean prettyPrint) {
		if(prettyPrint) {
			json = JSON.std.with(JSON.Feature.PRETTY_PRINT_OUTPUT);
		} else {
			json = JSON.std.without(JSON.Feature.PRETTY_PRINT_OUTPUT);
		}
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, T data) {
		try {
			return json.asBytes(data);
		} catch (IOException e) {
			throw new RuntimeException(String.format("Unable to generate a json string from %s (class:%s) -> %s", data.toString(), data.getClass().getName(), e));
		}
	}

	@Override
	public void close() {
	}

}
