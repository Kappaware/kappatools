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

import com.kappaware.kappatools.kcommon.Stats;
import com.kappaware.kcons.config.Configuration;

public class Engine extends Thread {
	Logger log = LoggerFactory.getLogger(Engine.class);

	private boolean running = true;
	private Configuration config;
	private KafkaConsumer<Object, Object> consumer;
	private Stats currentStats;
	private Stack<Stats> history = new Stack<Stats>();
	private boolean dumpMessage = false;
	private long lastPrintStats = 0L;

	public Engine(Configuration config) {
		this.config = config;
		consumer = new KafkaConsumer<Object, Object>(config.getConsumerProperties());
		this.dumpMessage = config.isMesson();
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
				if (this.dumpMessage) {
					System.out.printf("part:offset = %d:%d, key = '%s', value = '%s'\n", record.partition(), record.offset(), record.key().toString(), record.value().toString());
				}
				this.currentStats.add(record.key(), record.partition(), record.offset());
			}
			this.printStats("", false);
		}
		this.consumer.commitSync();

	}


	void printStats(String prefix, boolean force) {
		long now = System.currentTimeMillis();
		if ((this.config.getStatsPeriod() != 0 && this.lastPrintStats + this.config.getStatsPeriod() < now) || force) {
			this.lastPrintStats = now;
			log.info(this.getCurrentStats().toString());
		}
	}

	
	void halt() {
		this.running = false;
		//this.interrupt();
	}

	void setDumpMessage(boolean dm) {
		this.dumpMessage = dm;
	}

	public Stats getCurrentStats() {
		return currentStats;
	}

	public Stack<Stats> getHistory() {
		return history;
	}
	
	
	
}
