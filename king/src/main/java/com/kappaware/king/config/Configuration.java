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

import java.net.InetSocketAddress;
import java.util.Properties;

import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.jetty.IpMatcher;

public interface Configuration {

	String getBrokers();

	String getTopic();

	Properties getProducerProperties();
	
	String getGateId();

	String getAdminEndpoint();

	String getAdminAllowedNetwork();

	Settings getSettings();

	String getClientId();

	int getKeyLevel();

	int getReadStep();

	int getMaxMessageSize();

	IpMatcher getAllowedNetworkMatcher();

	InetSocketAddress getMainBindAddress();

}
