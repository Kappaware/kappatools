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
package com.kappaware.kcons;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.Utils;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.stats.AbstractStats;
import com.kappaware.kcons.config.Configuration;

public class EngineImpl extends Thread implements Engine {
	Logger log = LoggerFactory.getLogger(EngineImpl.class);

	private boolean running = true;
	private Configuration config;
	private KafkaConsumer<byte[], byte[]> consumer;
	private Stats stats = new Stats();
	private long lastSampling = 0L;
	private Settings settings;

	
	public EngineImpl(Configuration config) {
		this.config = config;
		consumer = new KafkaConsumer<byte[], byte[]>(config.getConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
		this.settings = config.getSettings();
	}

	@Override
	public void run() {
		consumer.subscribe(Arrays.asList(new String[] { config.getTopic() }), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				log.info(String.format("ConsumerRebalanceListener - Revoked partitions: %s", partitions.stream().map(TopicPartition::partition).collect(Collectors.toList())));
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				log.info(String.format("ConsumerRebalanceListener - Assigned partitions: %s", partitions.stream().map(TopicPartition::partition).collect(Collectors.toList())));
				stats.newConsumerStats(partitions);
			}
		});

		while (running) {
			ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
			for (ConsumerRecord<byte[], byte[]> record : records) {
				if (settings.getMesson()) {
					log.info(String.format("part:offset=%d:%d, timestamp=%s, key='%s', value='%s'\n", record.partition(), record.offset(), Utils.printIsoDateTime(record.timestamp()), new String(record.key()), new String(record.value())));
				}
				stats.addToConsumerStats(record.key(), record.partition(), record.offset());
			}
			this.updateStats("", false);
		}
		this.consumer.commitSync();
		this.consumer.close();
	}
	
	void updateStats(String prefix, boolean forceDisplay) {
		long now = System.currentTimeMillis();
		if (this.lastSampling + this.settings.getSamplingPeriod() < now) {
			this.lastSampling = now;
			this.stats.tick();
			if(this.settings.getStatson()) {
				log.info(this.stats.getConsumerStats().toString());
			}
		}
	}

	void halt() {
		this.running = false;
	}

	
	@Override
	public AbstractStats getStats() {
		return this.stats;
	}

	@Override
	public Settings getSettings() {
		return this.settings;
	}
	
}
