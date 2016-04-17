package com.kappaware.kgen.config;

import java.util.Properties;

public interface Configuration {

	String getTargetBrokers();

	String getTargetTopic();

	Properties getProducerProperties();
	
	long getInitialCounter();

	int getBurstCount();

	String getGateId();

	long getPeriod();

	long getStatsPeriod();


}
