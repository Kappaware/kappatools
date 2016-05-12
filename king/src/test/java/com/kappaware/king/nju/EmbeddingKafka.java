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
package com.kappaware.king.nju;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.jetty.server.Server;

import com.kappaware.king.EngineImpl;
import com.kappaware.king.Main;
import com.kappaware.king.config.Configuration;
import com.kappaware.king.config.ConfigurationImpl;
import com.kappaware.king.config.ParametersImpl;

import info.batey.kafka.unit.KafkaUnit;

public class EmbeddingKafka {
	static final int KAFKA_PORT = 5000;
	static final int ZK_PORT = 5001;
	static final String TOPIC = "topic1";
	static final int PARTITIONS_COUNT = 5;
	
	KafkaUnit kafkaUnit;
	String brokerList;
	KafkaConsumer<String, String> consumer;

	public void setUpKafka() {
		brokerList = String.format("localhost:%d", KAFKA_PORT);
		kafkaUnit = new KafkaUnit(ZK_PORT, KAFKA_PORT);
		kafkaUnit.startup();
		kafkaUnit.createTopic(TOPIC, PARTITIONS_COUNT);
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(TOPIC));
	}
	
	public void shutdownKafka() {
		consumer.close();
		kafkaUnit.shutdown();
	}
	
	public void test1() throws Exception {

		String[] argv = { "--brokers", brokerList, "--topic", TOPIC, "--gateId", "gate1", "--endpoint", "0.0.0.0:2017", "--keyLevel", "3" };

		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		Server server = Main.buildMainServer(config, new EngineImpl(config));
		server.start();

		System.out.printf("\n======================================================================\n");
		boolean run = true;
		while (run) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("----------------- partition = %d, offset = %d, key = %s, value = %s", record.partition(), record.offset(), record.key(), record.value());
				if(record.value().startsWith("stop")) {
					run = false;
				}
			}
		}
		System.out.printf("\n***********************************************************************\n");

		server.stop();

	}

	static public void main(String[] argv) throws Exception {

		EmbeddingKafka engine = new EmbeddingKafka();
		engine.setUpKafka();
		engine.test1();
		engine.shutdownKafka();
	}

}
