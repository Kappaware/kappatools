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
package com.kappaware.k2jdbc;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
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
import com.kappaware.k2jdbc.config.Configuration;
import com.kappaware.k2jdbc.jdbc.DbCatalog.DbColumnDef;
import com.kappaware.k2jdbc.jdbc.DbCatalog.DbTableDef;
import com.kappaware.k2jdbc.jdbc.DbEngine;
import com.kappaware.k2jdbc.jdbc.DbEngineImpl;
import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.jetty.AdminServer;
import com.kappaware.kappatools.kcommon.stats.AbstractStats;

public class EngineImpl extends Thread implements Engine {
	private static final int BATCH_SIZE = 2000;

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
	private JSON json = JSON.std;
	private String offsetQuery;
	private AdminServer adminServer = null;
	


	public EngineImpl(Configuration config) throws ConfigurationException, SQLException {
		this.config = config;
		consumer = new KafkaConsumer<byte[], byte[]>(config.getConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
		this.settings = config.getSettings();
		this.dbEngine = new DbEngineImpl(this.config.getTargetDataSource());
		DbTableDef tableDef = this.dbEngine.getDbCatalog().getTableDef(this.config.getTargetTable());
		if (tableDef == null) {
			throw new ConfigurationException(String.format("Table '%s' does not exist in this database", this.config.getTargetTable()));
		}
		DbColumnDef cd;
		if ((cd = tableDef.getColumnDef(KFK_TOPIC)) == null || (cd.getJdbcType() != Types.CHAR && cd.getJdbcType() != Types.VARCHAR)) {
			throw new ConfigurationException(String.format("Table '%s' need a column '%s' of type VARCHAR", this.config.getTargetTable(), KFK_TOPIC));
		}
		if ((cd = tableDef.getColumnDef(KFK_PARTITION)) == null || (cd.getJdbcType() != Types.TINYINT && cd.getJdbcType() != Types.INTEGER && cd.getJdbcType() != Types.BIGINT)) {
			throw new ConfigurationException(String.format("Table '%s' need a column '%s' of type INTEGER", this.config.getTargetTable(), KFK_PARTITION));
		}
		if ((cd = tableDef.getColumnDef(KFK_OFFSET)) == null || (cd.getJdbcType() != Types.BIGINT)) {
			throw new ConfigurationException(String.format("Table '%s' need a column '%s' of type BIGINT", this.config.getTargetTable(), KFK_OFFSET));
		}
		this.offsetQuery = String.format("SELECT MAX(%s) AS max_offset FROM %s WHERE %s = '%s' AND %s = ?", KFK_OFFSET, this.config.getTargetTable(), KFK_TOPIC, this.config.getTopic(), KFK_PARTITION);
	}

	/**
	 * We need to set eventual adminServer, to stop it in case of end of run. (As AdminServer is not a daemon, the program will still run in such case)
	 * @param adminServer
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
				log.debug("Will lookup offset in the target table");
				for (TopicPartition tp : partitions) {
					try {
						List<Map<String, Object>> result = dbEngine.query(offsetQuery, new Object[] { tp.partition() });
						Long offset = null;
						if (result != null && result.size() >= 1 && (offset = (Long) result.get(0).get("max_offset")) != null) {
							log.debug(String.format("Will seek to %d for partition %d", offset +1, tp.partition()));
							consumer.seek(tp, offset + 1);
						} else {
							log.debug(String.format("Will seek to beginning for partition %d", tp.partition()));
							consumer.seekToBeginning(tp);
						}
					} catch (Exception e) {
						running = false;
						throw new RuntimeException(String.format("Unable to read offset from database (request:%s)", offsetQuery), e);
					}
				}
			}
		});

		while (running) {
			ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
			List<Map<String, Object>> dataSet = new Vector<Map<String, Object>>();
			for (ConsumerRecord<byte[], byte[]> record : records) {
				if (settings.getMesson()) {
					log.info(String.format("part:offset = %d:%d, key = '%s', value = '%s'\n", record.partition(), record.offset(), new String(record.key()), new String(record.value())));
				}
				stats.addToConsumerStats(record.key(), record.partition(), record.offset());
				dataSet.add(this.buildDbRecord(record));
				if (dataSet.size() >= BATCH_SIZE) {
					write(dataSet);
					dataSet = new Vector<Map<String, Object>>();
				}
			}
			if (dataSet.size() > 0) {
				write(dataSet);
			}
			this.updateStats();
		}
		this.consumer.close();
		this.dbEngine.close();
		if(this.adminServer != null) { 
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
		// Add specific mandatory kafka fields
		dbRecord.put(KFK_TOPIC, this.config.getTopic());
		dbRecord.put(KFK_PARTITION, kfkRecord.partition());
		dbRecord.put(KFK_OFFSET, kfkRecord.offset());
		// We add some fields. They will be unused if not present in the table during the write.
		dbRecord.put(KFK_VALUE, kfkRecord.value());
		dbRecord.put(KFK_KEY, kfkRecord.key());
		try {
			Map<String, Object> key = json.mapFrom(kfkRecord.key());
			dbRecord.put(KEY_PREFIX, key);
		} catch (IOException e) {
			log.debug(String.format("Unable to parse '%s' as a JSON message", Arrays.toString(kfkRecord.key())));
		}
		Map<String, Object> flattenDbRecord = new HashMap<String, Object>();
		flatMap(flattenDbRecord, dbRecord, "");
		return flattenDbRecord;
	}

	@SuppressWarnings("unchecked")
	private void flatMap(Map<String, Object> target, Map<String, Object> current, String prefix) {
		for (Map.Entry<String, Object> entry : current.entrySet()) {
			if (entry.getValue() instanceof Map<?, ?>) {
				flatMap(target, (Map<String, Object>) entry.getValue(), entry.getKey() + "_");
			} else {
				if (target.containsKey(prefix + entry.getKey())) {
					log.warn(String.format("Name clash on '%s' when flattening record. Some data may be lost", prefix + entry.getKey()));
				} else {
					target.put((prefix + entry.getKey()).toLowerCase(), entry.getValue());
				}
			}
		}
	}

	private void write(List<Map<String, Object>> dataSet) {
		try {
			dbEngine.write(this.config.getTargetTable(), dataSet);
		} catch (SQLException | IOException e) {
			log.error("Exception while writing in db...", e);
			this.running = false;
		}
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
