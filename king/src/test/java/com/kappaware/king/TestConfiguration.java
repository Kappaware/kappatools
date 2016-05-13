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
package com.kappaware.king;

import java.net.InetSocketAddress;
import java.util.Properties;

import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.jetty.IpMatcher;
import com.kappaware.king.config.Configuration;
import com.kappaware.king.config.ConfigurationImpl;
import com.kappaware.king.config.ParametersImpl;

import junit.framework.TestCase;

public class TestConfiguration extends TestCase {
	static Properties properties1;
	static Properties properties2;
	
	static {
		properties1 = new Properties();
		properties1.put("bootstrap.servers", "b1:9000,b2:9000");

		properties2 = new Properties();
		properties2.put("bootstrap.servers", "b1:9000,b2:9000");
		properties2.put("compression.type", "gzip");
		properties2.put("linger.ms", "500");

	
	}
	

	public void testDefaults() throws ConfigurationException {
		String[] argv = {"--brokers", "b1:9000,b2:9000", "--gateId", "gate1", "--topic", "topic1" };
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		
		assertEquals("b1:9000,b2:9000", config.getBrokers());
		assertEquals("topic1", config.getTopic());
		assertEquals("gate1", config.getGateId());
		
		assertEquals("/0.0.0.0", config.getMainBindAddress().getAddress().toString());
		assertEquals(7070, config.getMainBindAddress().getPort());
		IpMatcher ipMatcher = config.getMainNetworkFilter();
		assertTrue(ipMatcher.match("1.1.1.1"));
		assertTrue(ipMatcher.match("200.1.1.1"));
		assertNull(config.getAdminBindAddress());
		assertEquals("gate1", config.getClientId());
		assertEquals(1, config.getKeyLevel());
		assertEquals(1000000, config.getMaxMessageSize());
		assertEquals(4096, config.getReadStep());
		assertFalse(config.getSettings().getMesson());
		assertFalse(config.getSettings().getStatson());
		assertEquals(new Long(1000L), config.getSettings().getSamplingPeriod());
		assertEquals(properties1, config.getProducerProperties());
	}

	public void testFull() throws ConfigurationException {
		String[] argv = {
				"--brokers", "b1:9000,b2:9000", 
				"--gateId", "gate1", 
				 "--topic", "topic1",
				 
				"--endpoint", "127.0.0.1:8888",
				"--allowedNetworks", "10.0.0.0/24,192.168.0.0/24,10.0.1.0/24",

				"--adminEndpoint", "127.0.0.1:8889",
				"--adminAllowedNetworks", "10.0.0.0/24,192.168.0.0/24,10.0.1.0/24",

				"--clientId", "theGateOne",
				"--keyLevel", "3",
				
				"--samplingPeriod", "2000",
				"--messon",
				"--statson",
				"--properties", "compression.type=gzip , linger.ms=500"
				
				
		};
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		
		assertEquals("b1:9000,b2:9000", config.getBrokers());
		assertEquals("topic1", config.getTopic());
		assertEquals("gate1", config.getGateId());
		
		assertEquals("/127.0.0.1", config.getMainBindAddress().getAddress().toString());
		assertEquals(8888, config.getMainBindAddress().getPort());
		assertEquals("theGateOne", config.getClientId());
		assertEquals(3, config.getKeyLevel());
		assertEquals(1000000, config.getMaxMessageSize());
		assertEquals(4096, config.getReadStep());
		assertTrue(config.getSettings().getMesson());
		assertTrue(config.getSettings().getStatson());
		assertEquals(new Long(2000L), config.getSettings().getSamplingPeriod());
		assertEquals(properties2, config.getProducerProperties());
		
		
		IpMatcher ipMatcher = config.getMainNetworkFilter();

		assertFalse(ipMatcher.match("1.1.1.1"));
		assertFalse(ipMatcher.match("200.1.1.1"));
		assertTrue(ipMatcher.match("10.0.0.12"));
		assertTrue(ipMatcher.match("10.0.1.12"));
		assertFalse(ipMatcher.match("10.0.2.12"));
		assertTrue(ipMatcher.match("192.168.0.12"));
		assertFalse(ipMatcher.match("192.168.1.12"));
		
		assertEquals(new InetSocketAddress("127.0.0.1", 8889), config.getAdminBindAddress());
		
	}
}
