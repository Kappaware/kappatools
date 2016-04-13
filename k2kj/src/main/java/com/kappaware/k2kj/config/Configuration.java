package com.kappaware.k2kj.config;

import java.util.Properties;

public interface Configuration {

	String getPartitionerField();

	String getSourceBrokers();

	String getTargetBrokers();

	String getSourceTopic();

	String getTargetTopic();

	String getConsumerGroup();

	Properties getConsumerProperties();

	Properties getProducerProperties();


}
