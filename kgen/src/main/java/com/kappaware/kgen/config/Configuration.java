package com.kappaware.kgen.config;

import java.util.Properties;

public interface Configuration {

	String getTargetBrokers();

	String getTargetTopic();

	Properties getProducerProperties();


}
