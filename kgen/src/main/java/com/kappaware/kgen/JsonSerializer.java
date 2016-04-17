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
