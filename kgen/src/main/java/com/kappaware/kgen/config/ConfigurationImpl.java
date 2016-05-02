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

package com.kappaware.kgen.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ConfigurationImpl implements Configuration {
	static Logger log = LoggerFactory.getLogger(ConfigurationImpl.class);

	// @formatter:off
	static Set<String> validConsumerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 
		ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 
		ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
		ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 
		ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 
		ConsumerConfig.METADATA_MAX_AGE_CONFIG,
		ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 
		ConsumerConfig.SEND_BUFFER_CONFIG, 
		ConsumerConfig.RECEIVE_BUFFER_CONFIG, 
		ConsumerConfig.CLIENT_ID_CONFIG, 
		ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 
		ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,
		ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 
		ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 
		ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
		ConsumerConfig.CHECK_CRCS_CONFIG, 
		ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 
		ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
		ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
		ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG
		
	}));

	static Set<String> protectedConsumerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ConsumerConfig.GROUP_ID_CONFIG, 
		ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
		ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, 
		ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
		ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
		ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
	}));

	// @formatter:off
	@SuppressWarnings("deprecation")
	static Set<String> validProducerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ProducerConfig.MAX_BLOCK_MS_CONFIG, 
		ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, 
		ProducerConfig.METADATA_MAX_AGE_CONFIG, 
		ProducerConfig.BATCH_SIZE_CONFIG, 
		ProducerConfig.BUFFER_MEMORY_CONFIG, 
		ProducerConfig.ACKS_CONFIG, 
		ProducerConfig.TIMEOUT_CONFIG, 
		ProducerConfig.LINGER_MS_CONFIG, 
		ProducerConfig.CLIENT_ID_CONFIG, 
		ProducerConfig.SEND_BUFFER_CONFIG, 
		ProducerConfig.RECEIVE_BUFFER_CONFIG, 
		ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 
		ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 
		ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, 
		ProducerConfig.RETRIES_CONFIG, 
		ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 
		ProducerConfig.COMPRESSION_TYPE_CONFIG, 
		ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 
		ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, 
		ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, 
		ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 
		ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 
		ProducerConfig.PARTITIONER_CLASS_CONFIG, 
		ProducerConfig.MAX_BLOCK_MS_CONFIG, 
		ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG 
	}));

	static Set<String> protectedProducerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
		ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
		ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG 
	}));

	
	
	
	// @formatter:on

	private Parameters parameters;
	private Properties producerProperties;

	public ConfigurationImpl(Parameters parameters) throws ConfigurationException {
		this.parameters = parameters;

		this.producerProperties = new Properties();
		this.producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.parameters.getBrokers());
		this.producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		this.producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

		if (this.parameters.getProperties() != null && this.parameters.getProperties().trim().length() > 0) {
			String[] sp = this.parameters.getProperties().split(",");
			for (String s : sp) {
				String[] prp = s.trim().split("=");
				if(prp.length != 2) {
					throw new ConfigurationException(String.format("Target property must be as name=value. Found '%s'", s));
				}
				String propName = prp[0].trim();
				if(this.parameters.isForceProperties()) {
					this.producerProperties.put(propName, prp[1].trim());
				} else {
					if(validProducerProperties.contains(propName)) {
						this.producerProperties.put(propName, prp[1].trim());
					} else if(protectedProducerProperties.contains(propName)) {
						throw new ConfigurationException(String.format("Usage of target property '%s' is reserved by this application!", propName));
					} else {
						throw new ConfigurationException(String.format("Invalid target property '%s'!", propName));
					}
				}
			}
		}
		
		

	}

	// ----------------------------------------------------------


	@Override
	public String getBrokers() {
		return parameters.getBrokers();
	}

	@Override
	public String getTopic() {
		return parameters.getTopic();
	}


	@Override
	public Properties getProducerProperties() {
		return this.producerProperties;
	}

	@Override
	public long getInitialCounter() {
		return this.parameters.getInitialCounter();
	}

	@Override
	public int getBurstCount() {
		return this.parameters.getBurstCount();
	}

	@Override
	public String getGateId() {
		return this.parameters.getGateId();
	}

	@Override
	public long getPeriod() {
		return this.parameters.getPeriod();
	}

	@Override
	public long getStatsPeriod() {
		return this.parameters.getStatsPeriod();
	}

	@Override
	public String getAdminEndpoint() {
		return this.parameters.getAdminEndpoint();
	}

	@Override
	public String getAdminAllowedNetwork() {
		return this.parameters.getAdminAllowedNetwork();
	}

}
