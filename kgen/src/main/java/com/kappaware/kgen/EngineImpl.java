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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jr.ob.JSON;
import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.ExtTs;
import com.kappaware.kappatools.kcommon.ExtTsFactory;
import com.kappaware.kappatools.kcommon.HeaderBuilder;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.stats.AbstractStats;
import com.kappaware.kgen.config.Configuration;
import com.kappaware.kgen.config.SettingsExt;

public class EngineImpl extends Thread implements Engine {
	static Logger log = LoggerFactory.getLogger(EngineImpl.class);

	private boolean running = true;
	private Configuration config;
	private KafkaProducer<String, String> producer;
	private ExtTsFactory factory;
	private HeaderBuilder headerBuilder;
	private long lastSampling = 0L;
	private Stats stats;
	private SettingsExt settings;
	private JSON json;

	EngineImpl(Configuration config) {
		this.config = config;
		this.producer = new KafkaProducer<String, String>(config.getProducerProperties(),  new StringSerializer(), new StringSerializer());
		this.factory = new ExtTsFactory(config.getGateId(), config.getInitialCounter());
		this.headerBuilder = new HeaderBuilder();
		this.stats = new Stats(producer.partitionsFor(this.config.getTopic()));
		this.settings = config.getSettings();
		this.json = JSON.std.without(JSON.Feature.PRETTY_PRINT_OUTPUT);
	}

	abstract class MyCallback implements Callback {
		ProducerRecord<String, String> record;
		MyCallback(ProducerRecord<String, String> record) {
			this.record = record;
		}
	}
	
	@Override
	public void run() {
		while (running) {
			int partitionCount = this.producer.partitionsFor(this.config.getTopic()).size();
			//log.trace(String.format("Partition count:%d", partitionCount));
			for (int i = 0; i < this.settings.getBurstCount(); i++) {
				ExtTs extTs = this.factory.get();
				Key key = new Key(extTs, headerBuilder);
				String keyString;
				try {
					keyString = json.asString(key);
				} catch (IOException e) {
					throw new RuntimeException(String.format("Unable to generate a json string from %s (class:%s) -> %s", key.toString(), key.getClass().getName(), e));
				}
				String value = String.format("Message #%d for %s from %s", extTs.getCounter(), key.getRecipient(), extTs.getGateId());
				int partition = Utils.abs(Utils.murmur2(key.getRecipient().getBytes())) % partitionCount;
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.config.getTopic(), partition, keyString, value);
				producer.send(record, new MyCallback(record) {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						stats.addToProducerStats(this.record.key().getBytes(), metadata.partition(), metadata.offset());
						if (settings.getMesson()) {
							log.info(String.format("part:offset = %d:%d, key = '%s', value = '%s'", metadata.partition(), metadata.offset(), record.key(), record.value()));
						}
					}
				});
			}
			try {
				Thread.sleep(this.settings.getPeriod());
			} catch (InterruptedException e) {
				log.debug("Interrupted in normal sleep!");
			}
			this.updateStats("", false);
		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// This case is normal in case of period = 0. In such case, interrupted flag was not cleared
			log.debug("Interrupted in end of thread processing!");
		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			log.warn("Too many interruption in end of thread processing!");
		}
		this.producer.flush();
		this.producer.close();
		this.updateStats("", true);
		log.info(String.format("Next counter:%d", this.factory.getNextCounter()));
	}

	void updateStats(String prefix, boolean forceDisplay) {
		long now = System.currentTimeMillis();
		if (this.lastSampling + this.settings.getSamplingPeriod() < now) {
			this.lastSampling = now;
			this.stats.tick();
			if(this.settings.getStatson()) {
				log.info(this.stats.getProducerStats().toString());
			}
		}
	}

	void halt() {
		this.running = false;
		this.interrupt();
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
