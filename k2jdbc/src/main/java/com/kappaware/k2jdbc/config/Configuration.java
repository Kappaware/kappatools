package com.kappaware.k2jdbc.config;

import java.net.InetSocketAddress;
import java.util.Properties;

import javax.sql.DataSource;

import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.jetty.IpMatcher;

public interface Configuration {

	String getBrokers();

	String getTopic();

	String getConsumerGroup();

	Properties getConsumerProperties();
	
	Settings getSettings();

	InetSocketAddress getAdminBindAddress();

	IpMatcher getAdminNetworkFilter();

	DataSource getTargetDataSource();

	String getTargetTable();

}
