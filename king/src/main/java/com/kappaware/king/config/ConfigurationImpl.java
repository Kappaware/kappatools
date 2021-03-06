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

package com.kappaware.king.config;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.Utils;
import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.jetty.IpMatcher;
import com.kappaware.kappatools.kcommon.jetty.IpMatcherImpl;

import org.apache.kafka.clients.producer.ProducerConfig;

public class ConfigurationImpl implements Configuration {
	static Logger log = LoggerFactory.getLogger(ConfigurationImpl.class);

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
		ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
		ProducerConfig.CLIENT_ID_CONFIG 
	}));

	
	
	
	// @formatter:on

	private ParametersImpl parameters;
	private Properties producerProperties;
	private Settings settings;
	private InetSocketAddress mainBindAddress;
	private IpMatcher mainNetworkFilter;
	private InetSocketAddress adminBindAddress;
	private IpMatcher adminNetworkFilter;


	public ConfigurationImpl(ParametersImpl parameters) throws ConfigurationException {
		this.parameters = parameters;

		this.producerProperties = new Properties();
		this.producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.parameters.getBrokers());
		//this.producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		//this.producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

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
		
		this.settings = new Settings(parameters);
		if(this.parameters.getKeyLevel() < 1 || this.parameters.getKeyLevel() > 3) {
			throw new ConfigurationException("keyLevel can only be 1, 2 or 3");
		}
		this.mainBindAddress = Utils.parseEndpoint(this.parameters.getEndpoint());
		this.mainNetworkFilter = new IpMatcherImpl(this.parameters.getAllowedNetworks());
		if(this.parameters.getAdminEndpoint() != null) {
			this.adminBindAddress = Utils.parseEndpoint(this.parameters.getAdminEndpoint());
		}
		this.adminNetworkFilter = new IpMatcherImpl(this.parameters.getAdminAllowedNetworks());	// Always defined, as there is a default value
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
	public String getGateId() {
		return this.parameters.getGateId();
	}

	@Override
	public InetSocketAddress getMainBindAddress() {
		return this.mainBindAddress;
	}

	@Override
	public IpMatcher getMainNetworkFilter() {
		return this.mainNetworkFilter;
	}

	
	@Override
	public InetSocketAddress getAdminBindAddress() {
		return adminBindAddress;
	}


	@Override
	public IpMatcher getAdminNetworkFilter() {
		return adminNetworkFilter;
	}

	@Override
	public Settings getSettings() {
		return settings;
	}

	@Override
	public String getClientId() {
		return ((parameters.getClientId() != null) ? this.parameters.getClientId() : this.getGateId());
	}


	@Override
	public int getKeyLevel() {
		return parameters.getKeyLevel();
	}

	// Currently hard coded. Is there a need to modify it ?
	@Override
	public int getReadStep() {
		return 4096;
	}

	// Currently hard coded. If someone want to increase, will need to adjust consumer properties AND broker config accordingly.
	@Override
	public int getMaxMessageSize() {
		return 1000000;
	}
	
}
