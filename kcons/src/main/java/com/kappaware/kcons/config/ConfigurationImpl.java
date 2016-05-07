package com.kappaware.kcons.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.config.Settings;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

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
		ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 
		ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,
		ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 
		ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 
		ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
		ConsumerConfig.CHECK_CRCS_CONFIG, 
		ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 
		ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
		ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
		ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
		ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
		ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
		
	}));

	static Set<String> protectedConsumerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ConsumerConfig.GROUP_ID_CONFIG, 
		ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
		ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
		ConsumerConfig.CLIENT_ID_CONFIG 
	}));

	
	
	// @formatter:on

	private ParametersImpl parameters;
	private Properties consumerProperties;
	private Settings settings;

	public ConfigurationImpl(ParametersImpl parameters) throws ConfigurationException {
		this.parameters = parameters;
		//log.debug(String.format("Source brokers:'%s'", this.getBrokers()));

		this.consumerProperties = new Properties();
		this.consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.parameters.getBrokers());
		this.consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, this.parameters.getConsumerGroup());
		this.consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		this.consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, this.parameters.getClientId());
		if (this.parameters.getProperties() != null && this.parameters.getProperties().trim().length() > 0) {
			String[] sp = this.parameters.getProperties().split(",");
			for (String s : sp) {
				String[] prp = s.trim().split("=");
				if(prp.length != 2) {
					throw new ConfigurationException(String.format("Source property must be as name=value. Found '%s'", s));
				}
				String propName = prp[0].trim();
				if(this.parameters.isForceProperties()) {
					this.consumerProperties.put(propName, prp[1].trim());
				} else {
					if(validConsumerProperties.contains(propName)) {
						this.consumerProperties.put(propName, prp[1].trim());
					} else if(protectedConsumerProperties.contains(propName)) {
						throw new ConfigurationException(String.format("Usage of source property '%s' is reserved by this application!", propName));
					} else {
						throw new ConfigurationException(String.format("Invalid source property '%s'!", propName));
					}
				}
			}
		}
		// Setup of some default properties
		if(!this.consumerProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
			this.consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		}
		if(!this.consumerProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
			this.consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		}
		if(!this.consumerProperties.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
			this.consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		}
		
		if(!this.consumerProperties.containsKey(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG) && !this.consumerProperties.containsKey(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)) {
			this.consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
			this.consumerProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2500");
		}
		this.settings = new Settings(parameters);

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
	public String getConsumerGroup() {
		return parameters.getConsumerGroup();
	}

	@Override
	public Properties getConsumerProperties() {
		return this.consumerProperties;
	}

	@Override
	public String getAdminEndpoint() {
		return parameters.getAdminEndpoint();
	}

	@Override
	public String getAdminAllowedNetwork() {
		return parameters.getAdminAllowedNetwork();
	}

	
	@Override
	public Settings getSettings() {
		return settings;
	}


}
