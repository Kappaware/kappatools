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
package com.kappaware.k2k;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jr.ob.JSON;
import com.kappaware.k2k.config.Configuration;
import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.stats.AbstractStats;

public class EngineImpl extends Thread implements Engine {
	Logger log = LoggerFactory.getLogger(EngineImpl.class);

	private static JSON json = JSON.std;

	private boolean running = true;
	private Configuration config;
	private KafkaConsumer<byte[], byte[]> consumer;
	private KafkaProducer<byte[], byte[]> producer;
	private Settings settings;
	private Stats stats;
	private long lastSampling = 0L;
	private int targetPartitionCount;

	public EngineImpl(Configuration config) throws ConfigurationException {
		this.config = config;
		this.consumer = new KafkaConsumer<byte[], byte[]>(config.getConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
		this.producer = new KafkaProducer<byte[], byte[]>(config.getProducerProperties(), new ByteArraySerializer(), new ByteArraySerializer());
		List<PartitionInfo> partitionInfo;
		try {
			partitionInfo = producer.partitionsFor(this.config.getTargetTopic());
		} catch (Throwable t) {
			throw new ConfigurationException(String.format("Unable to connect to target topic '%s' (brokers:'%s')", this.config.getTargetTopic(), this.config.getTargetBrokers()));
		}
		this.stats = new Stats(partitionInfo);
		this.targetPartitionCount = partitionInfo.size();
		this.settings = config.getSettings();
	}

	abstract class MyCallback implements Callback {
		ConsumerRecord<byte[], byte[]> consumerRecord;

		MyCallback(ConsumerRecord<byte[], byte[]> consumerRecord) {
			this.consumerRecord = consumerRecord;
		}
	}

	public void run() {
		consumer.subscribe(Arrays.asList(new String[] { config.getSourceTopic() }), new ConsumerRebalanceListener() {

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
			for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
				this.stats.addToConsumerStats(consumerRecord.key(), consumerRecord.partition(), consumerRecord.offset());
				int targetPartition = this.computeTargetPartition(consumerRecord.key());
				ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<byte[], byte[]>(this.config.getTargetTopic(), targetPartition, consumerRecord.timestamp(), consumerRecord.key(), consumerRecord.value());
				producer.send(producerRecord, new MyCallback(consumerRecord) {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						stats.addToProducerStats(this.consumerRecord.key(), metadata.partition(), metadata.offset());
						if (settings.getMesson()) {
							log.info(String.format("%d:%d => %d:%d, timestamp=%s, key='%s', value='%s'", consumerRecord.partition(), consumerRecord.offset(), metadata.partition(), metadata.offset(), com.kappaware.kappatools.kcommon.Utils.printIsoDateTime(consumerRecord.timestamp()), new String(consumerRecord.key()), new String(consumerRecord.value())));
						}
					}
				});
			}
			this.updateStats(false);
		}
		this.producer.flush();
		this.producer.close();
		this.consumer.commitSync();
		this.consumer.close();
	}

	static class Pk {
		private String partitionKey;

		public String getPartitionKey() {
			return partitionKey;
		}

		public void setPartitionKey(String partitionKey) {
			this.partitionKey = partitionKey;
		}
	}

	private int computeTargetPartition(byte[] key) {
		byte[] pselector = null;
		try {
			Pk pk = json.beanFrom(Pk.class, new String(key));
			pselector = pk.getPartitionKey().getBytes();
		} catch (IOException e) {
			// Key does not contains a json with a partitionKey field. Use all key to select partition
			pselector = key;
		}
		return Utils.abs(Utils.murmur2(pselector)) % this.targetPartitionCount;
	}

	void updateStats(boolean forceDisplay) {
		long now = System.currentTimeMillis();
		if (this.lastSampling + this.settings.getSamplingPeriod() < now) {
			this.lastSampling = now;
			this.stats.tick();
			if (this.settings.getStatson()) {
				log.info("Source: " + this.stats.getConsumerStats().toString());
				log.info("Target: " + this.stats.getProducerStats().toString());
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
