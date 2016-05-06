package com.kappaware.k2k.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.config.Parameters;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class ParametersImpl implements Parameters {
	static Logger log = LoggerFactory.getLogger(ParametersImpl.class);

	private String sourceBrokers;
	private String targetBrokers;
	private String sourceTopic;
	private String targetTopic;
	private String consumerGroup;

	private String partitionerField;
	private String sourceProperties;
	private String targetProperties;
	private boolean forceProperties;

	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120,2));
	}
	

	static OptionSpec<String> SOURCE_BROKERS_OPT = parser.accepts("sourceBrokers", "Comma separated values of Source Kafka brokers").withRequiredArg().describedAs("br1:9092,br2:9092").ofType(String.class).required();
	static OptionSpec<String> TARGET_BROKERS_OPT = parser.accepts("targetBrokers", "Comma separated values of Target Kafka brokers").withRequiredArg().describedAs("br8:9092,br9:9092").ofType(String.class).required();
	static OptionSpec<String> SOURCE_TOPIC_OPT = parser.accepts("sourceTopic", "Source topic").withRequiredArg().describedAs("topic1").ofType(String.class).required();
	static OptionSpec<String> TARGET_TOPIC_OPT = parser.accepts("targetTopic", "Target topic").withRequiredArg().describedAs("topic2").ofType(String.class).required();
	static OptionSpec<String> CONSUMER_GROUP_OPT = parser.accepts("consumerGroup", "Consumer group").withRequiredArg().describedAs("consGrp1").ofType(String.class).required();
	
	static OptionSpec<String> PARTITIONER_FIELD_OPT = parser.accepts("partitionerField", "JSON Path for partitioner field").withRequiredArg().describedAs("JSON path").ofType(String.class);
	static OptionSpec<String> SOURCE_PROPERTIES_OPT = parser.accepts("sourceProperties", "Source (Consumer) properties").withRequiredArg().describedAs("prop1=val1,prop2=val2").ofType(String.class);
	static OptionSpec<String> TARGET_PROPERTIES_OPT = parser.accepts("targetProperties", "Target (Producer) properties").withRequiredArg().describedAs("prop1=val1,prop2=val2").ofType(String.class);
	static OptionSpec<?> FORCE_PROPERTIES_OPT = parser.accepts("forceProperties", "Force unsafe properties");
	

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
			this.targetBrokers = result.valueOf(TARGET_BROKERS_OPT);
			this.sourceTopic = result.valueOf(SOURCE_TOPIC_OPT);
			this.targetTopic = result.valueOf(TARGET_TOPIC_OPT);
			this.consumerGroup = result.valueOf(CONSUMER_GROUP_OPT);
			this.partitionerField = result.valueOf(PARTITIONER_FIELD_OPT);
			this.sourceProperties = result.valueOf(SOURCE_PROPERTIES_OPT);
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

	public String getPartitionerField() {
		return partitionerField;
	}

	public String getSourceBrokers() {
		return sourceBrokers;
	}

	public String getTargetBrokers() {
		return targetBrokers;
	}

	public String getSourceTopic() {
		return sourceTopic;
	}

	public String getTargetTopic() {
		return targetTopic;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public String getSourceProperties() {
		return sourceProperties;
	}

	public String getTargetProperties() {
		return targetProperties;
	}

	public boolean isForceProperties() {
		return forceProperties;
	}

	@Override
	public long getSamplingPeriod() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isStatson() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isMesson() {
		// TODO Auto-generated method stub
		return false;
	}


}
