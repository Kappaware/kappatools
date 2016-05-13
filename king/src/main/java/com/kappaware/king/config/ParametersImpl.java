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

	private String brokers;
	private String topic;
	private String properties;
	private boolean forceProperties;
	private String clientId;

	private long samplingPeriod;
	private boolean statson;
	private boolean messon;

	private String adminEndpoint;
	private String adminAllowedNetworks;

	private String endpoint;
	private String allowedNetworks;
	private String gateId;
	private int keyLevel;

	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120,2));
	}
	
	@SuppressWarnings("serial")
	private static class MyOptionException extends Exception {
		public MyOptionException(String message) {
			super(message);
		}
	}

	
	static OptionSpec<String> BROKERS_OPT = parser.accepts("brokers", "Comma separated values of Target Kafka brokers").withRequiredArg().describedAs("br1:9092,br2:9092").ofType(String.class).required();
	static OptionSpec<String> TOPIC_OPT = parser.accepts("topic", "Target topic").withRequiredArg().describedAs("topic").ofType(String.class).required();
	static OptionSpec<String> PROPERTIES_OPT = parser.accepts("properties", "Producer properties").withRequiredArg().describedAs("prop1=val1,prop2=val2").ofType(String.class);
	static OptionSpec<?> FORCE_PROPERTIES_OPT = parser.accepts("forceProperties", "Force unsafe properties");
	static OptionSpec<String> CLIENT_ID_OPT = parser.accepts("clientId", "Client ID (default: gateId value)").withRequiredArg().describedAs("client1").ofType(String.class);

	static OptionSpec<Long> SAMPLING_PERIOD_OPT = parser.accepts("samplingPeriod", "Throughput sampling and stats diplay period (ms)").withRequiredArg().describedAs("samplingPeriod(ms)").ofType(Long.class).defaultsTo(1000L);
	static OptionSpec<?> STATSON_OPT = parser.accepts("statson", "Display stats on sampling period");
	static OptionSpec<?> MESSON_OPT = parser.accepts("messon", "Display all read messages");

	static OptionSpec<String> ADMIN_ENDPOINT_OPT = parser.accepts("adminEndpoint", "Admin REST endoint (default: none)").withRequiredArg().describedAs("[Interface:]port").ofType(String.class);
	static OptionSpec<String> ADMIN_ALLOWED_NETWORKS_OPT = parser.accepts("adminAllowedNetworks", "Admin allowed network").withRequiredArg().describedAs("net1/cidr1,net2/cidr2,...").ofType(String.class).defaultsTo("127.0.0.1/32");

	static OptionSpec<String> ENDPOINT_OPT = parser.accepts("endpoint", "Main REST endpoint").withRequiredArg().describedAs("[Interface:]port").ofType(String.class).defaultsTo("0.0.0.0:7070");
	static OptionSpec<String> ALLOWED_NETWORKS_OPT = parser.accepts("allowedNetworks", "Main allowed network").withRequiredArg().describedAs("net1/cidr1,net2/cidr2,...").ofType(String.class).defaultsTo("0.0.0.0/0");;
	static OptionSpec<String> GATE_ID_OPT = parser.accepts("gateId", "generator Id. Must be unique").withRequiredArg().describedAs("someId").ofType(String.class).required();
	static OptionSpec<Integer> KEY_LEVEL_OPT = parser.accepts("keyLevel", "Key level").withRequiredArg().describedAs("1|2|3").ofType(Integer.class).defaultsTo(1);

	public ParametersImpl(String[] argv) throws ConfigurationException {
		try {
			OptionSet result = parser.parse(argv);

			if (result.nonOptionArguments().size() > 0 && result.nonOptionArguments().get(0).toString().trim().length() > 0) {
				throw new MyOptionException(String.format("Unknow option '%s'", result.nonOptionArguments().get(0)));
			}
			// Mandatories parameters
			this.brokers = result.valueOf(BROKERS_OPT);
			this.topic = result.valueOf(TOPIC_OPT);
			this.properties = result.valueOf(PROPERTIES_OPT);
			this.forceProperties = result.has(FORCE_PROPERTIES_OPT);
			this.clientId = result.valueOf(CLIENT_ID_OPT);

			this.samplingPeriod = result.valueOf(SAMPLING_PERIOD_OPT);
			this.statson = result.has(STATSON_OPT);
			this.messon = result.has(MESSON_OPT);

			this.adminEndpoint = result.valueOf(ADMIN_ENDPOINT_OPT);
			this.adminAllowedNetworks = result.valueOf(ADMIN_ALLOWED_NETWORKS_OPT);

			this.endpoint = result.valueOf(ENDPOINT_OPT);
			this.allowedNetworks = result.valueOf(ALLOWED_NETWORKS_OPT);
			this.gateId = result.valueOf(GATE_ID_OPT);
			this.keyLevel = result.valueOf(KEY_LEVEL_OPT);
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

	
	@Override
	public long getSamplingPeriod() {
		return this.samplingPeriod;
	}

	public String getBrokers() {
		return brokers;
	}

	public String getTopic() {
		return topic;
	}

	public String getProperties() {
		return properties;
	}

	public boolean isForceProperties() {
		return forceProperties;
	}

	public String getClientId() {
		return clientId;
	}

	public String getAdminEndpoint() {
		return adminEndpoint;
	}

	public String getAdminAllowedNetworks() {
		return adminAllowedNetworks;
	}

	public String getGateId() {
		return gateId;
	}

	@Override
	public boolean isStatson() {
		return this.statson;
	}

	@Override
	public boolean isMesson() {
		return this.messon;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getAllowedNetworks() {
		return allowedNetworks;
	}

	public int getKeyLevel() {
		return keyLevel;
	}

}
