package com.kappaware.kcons.config;

import java.util.Properties;

import com.kappaware.kappatools.kcommon.config.Settings;

public interface Configuration {

	String getBrokers();

	String getTopic();

	String getConsumerGroup();

	Properties getConsumerProperties();
	
	Settings getSettings();

	String getAdminEndpoint();

	String getAdminAllowedNetwork();
}
