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
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.Stats;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kcons.config.Configuration;

public class EngineImpl extends Thread implements Engine {
	Logger log = LoggerFactory.getLogger(EngineImpl.class);

	private boolean running = true;
	private Configuration config;
	private KafkaConsumer<Object, Object> consumer;
	private Stats currentStats;
	private Stack<Stats> history = new Stack<Stats>();
	private long lastSampling = 0L;
	private Settings settings;

	
	public EngineImpl(Configuration config) {
		this.config = config;
		consumer = new KafkaConsumer<Object, Object>(config.getConsumerProperties());
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
				if (currentStats != null) {
					history.push(currentStats);
				}
				currentStats = new Stats(partitions);
			}
		});

		while (running) {
			ConsumerRecords<Object, Object> records = consumer.poll(100);
			for (ConsumerRecord<Object, Object> record : records) {
				if (settings.getMesson()) {
					log.info(String.format("part:offset = %d:%d, key = '%s', value = '%s'\n", record.partition(), record.offset(), record.key().toString(), record.value().toString()));
				}
				this.currentStats.add(record.key(), record.partition(), record.offset());
			}
			this.updateStats("", false);
		}
		this.consumer.commitSync();

	}
	
	void updateStats(String prefix, boolean forceDisplay) {
		long now = System.currentTimeMillis();
		if (this.lastSampling + this.settings.getSamplingPeriod() < now) {
			this.lastSampling = now;
			this.getStats().tick();
			if(this.settings.getStatson()) {
				log.info(this.getStats().toString());
			}
		}
	}

	void halt() {
		this.running = false;
		//this.interrupt();
	}

	public Stats getCurrentStats() {
		return currentStats;
	}

	public Stack<Stats> getHistory() {
		return history;
	}

	@Override
	public Stats getStats() {
		return this.currentStats;
	}

	@Override
	public Settings getSettings() {
		return this.settings;
	}
	
}
