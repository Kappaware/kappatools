package com.kappaware.kcons.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class Parameters {
	static Logger log = LoggerFactory.getLogger(Parameters.class);

	private String brokers;
	private String topic;
	private String consumerGroup;
	private String clientId;

	private String properties;
	private boolean forceProperties;
	private boolean keyboard;
	private int statsPeriod;
	
	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120,2));
	}
	

	static OptionSpec<String> BROKERS_OPT = parser.accepts("brokers", "Comma separated values of Source Kafka brokers").withRequiredArg().describedAs("br1:9092,br2:9092").ofType(String.class).required();
	static OptionSpec<String> STOPIC_OPT = parser.accepts("topic", "Source topic").withRequiredArg().describedAs("topic1").ofType(String.class).required();
	static OptionSpec<String> CONSUMER_GROUP_OPT = parser.accepts("consumerGroup", "Consumer group").withRequiredArg().describedAs("consGrp1").ofType(String.class).required();
	static OptionSpec<String> CLIENT_ID_OPT = parser.accepts("clientId", "Client ID").withRequiredArg().describedAs("client1").ofType(String.class).required();
	
	static OptionSpec<String> SOURCE_PROPERTIES_OPT = parser.accepts("properties", "Consumer properties").withRequiredArg().describedAs("prop1=val1,prop2=val2").ofType(String.class);
	static OptionSpec<?> FORCE_PROPERTIES_OPT = parser.accepts("forceProperties", "Force unsafe properties");
	static OptionSpec<?> KEYBOARD_PROPERTIES_OPT = parser.accepts("keyboard", "Allow keyboard interaction");
	static OptionSpec<Integer> STATS_PERIOD_OPT = parser.accepts("statsPeriod", "Period between stats display (ms) (0: no stats)").withRequiredArg().describedAs("statsPeriod(ms)").ofType(Integer.class).defaultsTo(1000);


	@SuppressWarnings("serial")
	private static class MyOptionException extends Exception {

		public MyOptionException(String message) {
			super(message);
		}
		
	}

	
	public Parameters(String[] argv) throws ConfigurationException {
		try {
			OptionSet result = parser.parse(argv);

			if (result.nonOptionArguments().size() > 0 && result.nonOptionArguments().get(0).toString().trim().length() > 0) {
				throw new MyOptionException(String.format("Unknow option '%s'", result.nonOptionArguments().get(0)));
			}
			// Mandatories parameters
			this.brokers = result.valueOf(BROKERS_OPT);
			this.topic = result.valueOf(STOPIC_OPT);
			this.consumerGroup = result.valueOf(CONSUMER_GROUP_OPT);
			this.properties = result.valueOf(SOURCE_PROPERTIES_OPT);
			this.forceProperties = result.has(FORCE_PROPERTIES_OPT);
			this.clientId = result.valueOf(CLIENT_ID_OPT);
			this.keyboard = result.has(KEYBOARD_PROPERTIES_OPT);
			this.statsPeriod = result.valueOf(STATS_PERIOD_OPT);

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

	public String getBrokers() {
		return brokers;
	}


	public String getTopic() {
		return topic;
	}

	public String getConsumerGroup() {
		return consumerGroup;
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

	public boolean isKeyboard() {
		return keyboard;
	}
	
	public int getStatsPeriod() {
		return statsPeriod;
	}


}
