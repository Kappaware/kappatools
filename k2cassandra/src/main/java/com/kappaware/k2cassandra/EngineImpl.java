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
package com.kappaware.k2cassandra;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jr.ob.JSON;
import com.kappaware.k2cassandra.cassandra.DbEngine;
import com.kappaware.k2cassandra.cassandra.DbEngine.DbTable;
import com.kappaware.k2cassandra.cassandra.DbEngineException;
import com.kappaware.k2cassandra.cassandra.DbEngineImpl;
import com.kappaware.k2cassandra.config.Configuration;
import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.Utils;
import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.jetty.AdminServer;
import com.kappaware.kappatools.kcommon.stats.AbstractStats;

public class EngineImpl extends Thread implements Engine {

	private static final String KFK_TOPIC = "kfk_topic";
	private static final String KFK_PARTITION = "kfk_partition";
	private static final String KFK_OFFSET = "kfk_offset";

	private static final String KFK_KEY = "kfk_key";
	private static final String KFK_VALUE = "kfk_value";

	private static final String KEY_PREFIX = "kfkey";

	static Logger log = LoggerFactory.getLogger(EngineImpl.class);

	private boolean running = true;
	private Configuration config;
	private KafkaConsumer<byte[], byte[]> consumer;
	private Stats stats = new Stats();
	private long lastSampling = 0L;
	private Settings settings;
	private DbEngine dbEngine;
	private DbTable dbTable;
	private JSON json = JSON.std;
	private AdminServer adminServer = null;
	private String kfk_topic;
	private String kfk_partition;
	private String kfk_offset;
	private String kfk_key;
	private String kfk_value;

	public EngineImpl(Configuration config) throws ConfigurationException {
		this.config = config;
		this.kfk_topic = this.mapColName(KFK_TOPIC);
		this.kfk_partition = this.mapColName(KFK_PARTITION);
		this.kfk_offset = this.mapColName(KFK_OFFSET);
		this.kfk_key = this.mapColName(KFK_KEY);
		this.kfk_value = this.mapColName(KFK_VALUE);
		consumer = new KafkaConsumer<byte[], byte[]>(config.getConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
		this.settings = config.getSettings();
		this.dbEngine = new DbEngineImpl(this.config.getCluster());
		try {
			this.dbTable = this.dbEngine.getTable(this.config.getTargetTable());
		} catch (DbEngineException e) {
			throw new ConfigurationException(e.getLocalizedMessage());
		}
	}

	/*
	 * We need to set eventual adminServer, to stop it in case of end of run. (As AdminServer is not a daemon, the program will still run in such case)
	 */
	public void setAdminServer(AdminServer adminServer) {
		this.adminServer = adminServer;
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
			try {
				ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
				for (ConsumerRecord<byte[], byte[]> record : records) {
					if (settings.getMesson()) {
						log.info(String.format("part:offset = %d:%d, key = '%s', value = '%s'\n", record.partition(), record.offset(), new String(record.key()), new String(record.value())));
					}
					stats.addToConsumerStats(record.key(), record.partition(), record.offset());
					this.dbTable.write(this.buildDbRecord(record));
				}
				this.updateStats();
			} catch (DbEngineException e) {
				log.error(String.format("Exception while writing row '%s' in db...", Utils.jsonPrettyString(e.getRow())), e);
				running = false;
			}
		}
		this.dbEngine.close();
		this.consumer.commitSync();
		this.consumer.close();
		if (this.adminServer != null) {
			try {
				this.adminServer.halt();
			} catch (Exception e) {
				log.error("Error in AdminServer.halt()", e);
			}
		}
	}

	Map<String, Object> buildDbRecord(ConsumerRecord<byte[], byte[]> kfkRecord) {
		Map<String, Object> dbRecord;
		// Base for dbrecord is the message value, if it is parseable as json.
		try {
			dbRecord = json.mapFrom(kfkRecord.value());
		} catch (IOException e) {
			log.trace(String.format("Unable to parse '%s' - '%s' as a JSON message", new String(kfkRecord.value()), Arrays.toString(kfkRecord.value())));
			dbRecord = new HashMap<String, Object>();
		}
		// Add specific kafka fields. NB: We use unmapped column name, as they will be mapped by flattenRecord
		// They will be unused if not present in the table during the write.
		dbRecord.put(kfk_topic, this.config.getTopic());
		dbRecord.put(kfk_partition, kfkRecord.partition());
		dbRecord.put(kfk_offset, kfkRecord.offset());
		dbRecord.put(kfk_value, kfkRecord.value());
		dbRecord.put(kfk_key, kfkRecord.key());
		try {
			Map<String, Object> key = json.mapFrom(kfkRecord.key());
			dbRecord.put(KEY_PREFIX, key);
		} catch (IOException e) {
			log.debug(String.format("Unable to parse '%s' as a JSON message", Arrays.toString(kfkRecord.key())));
		}
		Map<String, Object> flattenDbRecord = new HashMap<String, Object>();
		//log.debug(String.format("Before fatten:%s", Utils.jsonPrettyString(dbRecord)));
		flattenRecord(flattenDbRecord, dbRecord, "");
		//log.debug(String.format("After fatten:%s", Utils.jsonPrettyString(flattenDbRecord)));
		return flattenDbRecord;
	}

	@SuppressWarnings("unchecked")
	private void flattenRecord(Map<String, Object> target, Map<String, Object> current, String prefix) {
		for (Map.Entry<String, Object> entry : current.entrySet()) {
			if (entry.getValue() instanceof Map<?, ?>) {
				flattenRecord(target, (Map<String, Object>) entry.getValue(), prefix + entry.getKey() + "_");
			} else {
				if (target.containsKey(prefix + entry.getKey())) {
					log.warn(String.format("Name clash on '%s' when flattening record. Some data may be lost", prefix + entry.getKey()));
				} else {
					target.put(this.mapColName((prefix + entry.getKey())), entry.getValue());
				}
			}
		}
	}

	private String mapColName(String fieldName) {
		String colName = this.config.getColMapping().get(this.config.isPreserveCase() ? fieldName : fieldName.toLowerCase());
		if (colName == null) {
			colName = fieldName;
		}
		return this.config.isPreserveCase() ? colName : colName.toLowerCase();
	}

	void updateStats() {
		long now = System.currentTimeMillis();
		if (this.lastSampling + this.settings.getSamplingPeriod() < now) {
			this.lastSampling = now;
			this.stats.tick();
			if (this.settings.getStatson()) {
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
