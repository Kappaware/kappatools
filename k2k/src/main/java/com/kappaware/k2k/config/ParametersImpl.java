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
package com.kappaware.k2k.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.config.Parameters;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ParametersImpl implements Parameters {
	static Logger log = LoggerFactory.getLogger(ParametersImpl.class);

	private String sourceBrokers;
	private String sourceTopic;
	private String sourceProperties;
	private String consumerGroup;

	private String targetBrokers;
	private String targetTopic;
	private String targetProperties;

	private boolean forceProperties;
	private String clientId;

	private long samplingPeriod;
	private boolean messon;
	private boolean statson;

	private String adminEndpoint;
	private String adminAllowedNetworks;

	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120,2));
	}
	

	static OptionSpec<String> SOURCE_BROKERS_OPT = parser.accepts("sourceBrokers", "Comma separated values of Source Kafka brokers").withRequiredArg().describedAs("br1:9092,br2:9092").ofType(String.class).required();
	static OptionSpec<String> SOURCE_TOPIC_OPT = parser.accepts("sourceTopic", "Source topic").withRequiredArg().describedAs("topic1").ofType(String.class).required();
	static OptionSpec<String> SOURCE_PROPERTIES_OPT = parser.accepts("sourceProperties", "Source (Consumer) properties").withRequiredArg().describedAs("prop1=val1,prop2=val2").ofType(String.class);
	static OptionSpec<String> CONSUMER_GROUP_OPT = parser.accepts("consumerGroup", "Consumer group").withRequiredArg().describedAs("consGrp1").ofType(String.class).required();

	static OptionSpec<String> TARGET_BROKERS_OPT = parser.accepts("targetBrokers", "Comma separated values of Target Kafka brokers").withRequiredArg().describedAs("br8:9092,br9:9092").ofType(String.class).required();
	static OptionSpec<String> TARGET_TOPIC_OPT = parser.accepts("targetTopic", "Target topic").withRequiredArg().describedAs("topic2").ofType(String.class).required();
	static OptionSpec<String> TARGET_PROPERTIES_OPT = parser.accepts("targetProperties", "Target (Producer) properties").withRequiredArg().describedAs("prop1=val1,prop2=val2").ofType(String.class);

	static OptionSpec<?> FORCE_PROPERTIES_OPT = parser.accepts("forceProperties", "Force unsafe properties");
	static OptionSpec<String> CLIENT_ID_OPT = parser.accepts("clientId", "Client ID").withRequiredArg().describedAs("client1").ofType(String.class).required();

	static OptionSpec<Long> SAMPLING_PERIOD_OPT = parser.accepts("samplingPeriod", "Throughput sampling and stats diplay period (ms)").withRequiredArg().describedAs("samplingPeriod(ms)").ofType(Long.class).defaultsTo(1000L);
	static OptionSpec<?> STATSON_OPT = parser.accepts("statson", "Display stats on sampling period");
	static OptionSpec<?> MESSON_OPT = parser.accepts("messon", "Display all read messages");

	static OptionSpec<String> ADMIN_ENDPOINT_OPT = parser.accepts("adminEndpoint", "Admin REST endoint").withRequiredArg().describedAs("[Interface:]port").ofType(String.class);
	static OptionSpec<String> ADMIN_ALLOWED_NETWORKS_OPT = parser.accepts("adminAllowedNetworks", "Admin allowed networks").withRequiredArg().describedAs("net1/cidr1,net2/cidr2,...").ofType(String.class).defaultsTo("127.0.0.1/32");;

	

	@SuppressWarnings("serial")
	private static class MyOptionException extends Exception {

		public MyOptionException(String message) {
			super(message);
		}
		
	}

	
	public ParametersImpl(String[] argv) throws ConfigurationException {
		try {
			OptionSet result = parser.parse(argv);

			if (result.nonOptionArguments().size() > 0 && result.nonOptionArguments().get(0).toString().trim().length() > 0) {
				throw new MyOptionException(String.format("Unknow option '%s'", result.nonOptionArguments().get(0)));
			}
			// Mandatories parameters
			this.sourceBrokers = result.valueOf(SOURCE_BROKERS_OPT);
			this.sourceTopic = result.valueOf(SOURCE_TOPIC_OPT);
			this.sourceProperties = result.valueOf(SOURCE_PROPERTIES_OPT);
			this.consumerGroup = result.valueOf(CONSUMER_GROUP_OPT);

			this.targetBrokers = result.valueOf(TARGET_BROKERS_OPT);
			this.targetTopic = result.valueOf(TARGET_TOPIC_OPT);
			this.targetProperties = result.valueOf(TARGET_PROPERTIES_OPT);

			this.forceProperties = result.has(FORCE_PROPERTIES_OPT);
			this.clientId = result.valueOf(CLIENT_ID_OPT);

			this.samplingPeriod = result.valueOf(SAMPLING_PERIOD_OPT);
			this.statson = result.has(STATSON_OPT);
			this.messon = result.has(MESSON_OPT);

			this.adminEndpoint = result.valueOf(ADMIN_ENDPOINT_OPT);
			this.adminAllowedNetworks = result.valueOf(ADMIN_ALLOWED_NETWORKS_OPT);
		} catch (OptionException | MyOptionException t) {
			throw new ConfigurationException(usage(t.getMessage()));
		}
	}

	private static String usage(String err) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(baos);
		if (err != null) {
			pw.print(String.format("\n\n * * * * * ERROR: %s\n\n", err));
		}
		try {
			parser.printHelpOn(pw);
		} catch (IOException e) {
		}
		pw.flush();
		pw.close();
		return baos.toString();
	}

	// --------------------------------------------------------------------------


	public String getSourceBrokers() {
		return sourceBrokers;
	}
	public String getSourceTopic() {
		return sourceTopic;
	}
	public String getSourceProperties() {
		return sourceProperties;
	}
	public String getConsumerGroup() {
		return consumerGroup;
	}

	public String getTargetBrokers() {
		return targetBrokers;
	}
	public String getTargetTopic() {
		return targetTopic;
	}
	public String getTargetProperties() {
		return targetProperties;
	}


	public boolean isForceProperties() {
		return forceProperties;
	}

	public String getClientId() {
		return clientId;
	}

	@Override
	public long getSamplingPeriod() {
		return this.samplingPeriod;
	}
	@Override
	public boolean isStatson() {
		return this.statson;
	}
	@Override
	public boolean isMesson() {
		return this.messon;
	}

	public String getAdminEndpoint() {
		return adminEndpoint;
	}
	public String getAdminAllowedNetworks() {
		return adminAllowedNetworks;
	}
}
