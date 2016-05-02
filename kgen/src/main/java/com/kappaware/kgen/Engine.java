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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kgen.config.Configuration;

public class Engine extends Thread {
	static Logger log = LoggerFactory.getLogger(Engine.class);

	private boolean running = true;
	private Configuration config;
	private KafkaProducer<Key, String> producer;
	private ExtTsFactory factory;
	private HeaderBuilder headerBuilder;
	private long lastPrintStats = 0L;
	private Stats stats;

	Engine(Configuration config) {
		this.config = config;
		this.producer = new KafkaProducer<Key, String>(config.getProducerProperties(), new JsonSerializer<Key>(false), new StringSerializer());
		this.factory = new ExtTsFactory(config.getGateId(), config.getInitialCounter());
		this.headerBuilder = new HeaderBuilder();
		this.stats = new Stats(producer.partitionsFor(this.config.getTopic()));
	}

	public void run() {
		while (running) {
			int partitionCount = this.producer.partitionsFor(this.config.getTopic()).size();
			//log.trace(String.format("Partition count:%d", partitionCount));

			for (int i = 0; i < config.getBurstCount(); i++) {
				ExtTs extTs = this.factory.get();
				Key key = new Key(extTs, headerBuilder);
				String value = String.format("Message #%d for %s from %s", extTs.getCounter(), key.getRecipient(), extTs.getGateId());
				int partition = Utils.abs(Utils.murmur2(key.getRecipient().getBytes())) % partitionCount;
				//log.trace(String.format("Pushing message to kafka to partition %d", partition));
				producer.send(new ProducerRecord<Key, String>(this.config.getTopic(), partition, key, value), new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						stats.add(key, metadata.partition(), metadata.offset());
					}
				});
			}
			try {
				Thread.sleep(config.getPeriod());
			} catch (InterruptedException e) {
				log.debug("Interrupted in normal sleep!");
			}
			this.printStats("", false);
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
		log.info("KGEN END");
		this.printStats("", true);
		log.info(String.format("Next counter:%d", this.factory.getNextCounter()));
	}

	void printStats(String prefix, boolean force) {
		long now = System.currentTimeMillis();
		if ((this.config.getStatsPeriod() != 0 && this.lastPrintStats + this.config.getStatsPeriod() < now) || force) {
			this.lastPrintStats = now;
			this.getStats().tick();
			log.info(this.getStats().toString());
		} else if(this.config.getStatsPeriod() == 0 && this.lastPrintStats + 1000 < now)  {
			this.lastPrintStats = now;
			this.getStats().tick();
		}
	}

	void halt() {
		this.running = false;
		this.interrupt();
	}

	public Stats getStats() {
		return this.stats;
	}

}
