package com.kappaware.kgen.config;

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

	private String targetBrokers;
	private String targetTopic;
	
	private String targetProperties;
	private boolean forceProperties;

	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120,2));
	}

	static OptionSpec<String> TARGET_BROKERS_OPT = parser.accepts("targetBrokers", "Comma separated values of Target Kafka brokers").withRequiredArg().describedAs("br8:9092,br9:9092").ofType(String.class).required();
	static OptionSpec<String> TARGET_TOPIC_OPT = parser.accepts("targetTopic", "Target topic").withRequiredArg().describedAs("topic2").ofType(String.class).required();
	
	static OptionSpec<String> TARGET_PROPERTIES_OPT = parser.accepts("targetProperties", "Target (Producer) properties").withRequiredArg().describedAs("prop1=val1,prop2=val2").ofType(String.class);
	static OptionSpec<?> FORCE_PROPERTIES_OPT = parser.accepts("forceProperties", "Force unsafe properties");
	

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
			this.targetBrokers = result.valueOf(TARGET_BROKERS_OPT);
			this.targetTopic = result.valueOf(TARGET_TOPIC_OPT);
			this.targetProperties = result.valueOf(TARGET_PROPERTIES_OPT);
			this.forceProperties = result.has(FORCE_PROPERTIES_OPT);
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


}
