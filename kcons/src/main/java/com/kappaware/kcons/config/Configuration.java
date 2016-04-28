package com.kappaware.kcons.config;

import java.util.Properties;

public interface Configuration {

	String getBrokers();

	String getTopic();

	String getConsumerGroup();

	Properties getConsumerProperties();

	boolean isKeyboard();

	long getStatsPeriod();

	boolean isMesson();


}
