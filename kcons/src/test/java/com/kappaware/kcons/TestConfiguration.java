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
package com.kappaware.kcons;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import com.kappaware.kcons.config.Configuration;
import com.kappaware.kcons.config.ConfigurationException;
import com.kappaware.kcons.config.ConfigurationImpl;
import com.kappaware.kcons.config.ParametersImpl;


public class TestConfiguration {
	static Properties sourceProperties1;
	static Properties targetProperties1;
	
	static {
		sourceProperties1 = new Properties();
		sourceProperties1.put("key.deserializer", StringDeserializer.class);
		sourceProperties1.put("value.deserializer", StringDeserializer.class);
		sourceProperties1.put("auto.offset.reset", "latest");
		sourceProperties1.put("bootstrap.servers", "xx:9092");
		sourceProperties1.put("enable.auto.commit", true);
		sourceProperties1.put("group.id", "grp1");
		sourceProperties1.put("client.id", "cli1");
		sourceProperties1.put("session.timeout.ms", "10000");
		sourceProperties1.put("heartbeat.interval.ms", "2500");
	}


	@Test
	public void test1() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1", "--consumerGroup", "grp1", "--clientId", "cli1" };

		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getBrokers());
		assertEquals("t1", config.getTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(sourceProperties1, config.getConsumerProperties());
	}

	
	@Test
	public void testMandatorySourceBroker() throws ConfigurationException {
		String[] argv = { "--topic", "t1", "--consumerGroup", "grp1", "--clientId", "cli1" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertTrue(e.getMessage().contains("Missing required option(s) [brokers]"));
			return;
		}
		fail("Missing source broker undetected");
	}
	// ------------------------------------------ Source properties
	@Test
	public void testInvalidSourceProperties1() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1", "--consumerGroup", "grp1", "--properties", "aa=xx", "--clientId", "cli1" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid source property 'aa'!", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}

	@Test
	public void testInvalidSourceProperties2() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1", "--consumerGroup", "grp1", "--clientId", "cli1", "--properties", "aa" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Source property must be as name=value. Found 'aa'", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}

	@Test
	public void testInvalidSourceProperties3() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1",  "--consumerGroup", "grp1", "--clientId", "cli1", "--properties", "aa=" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Source property must be as name=value. Found 'aa='", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}
	
	@Test
	public void testInvalidSourceProperties4() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1", "--consumerGroup", "grp1", "--clientId", "cli1", "--properties", "=xx" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid source property ''!", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}
	
	
	@Test
	public void testEmptySourceProperties5() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1", "--consumerGroup", "grp1", "--clientId", "cli1", "--properties", "" };
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getBrokers());
		assertEquals("t1", config.getTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(sourceProperties1, config.getConsumerProperties());
	}
	

	@Test
	public void testForceInvalidSourceProperties() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1", "--consumerGroup", "grp1", "--clientId", "cli1", "--properties", "aa=xx", "--forceProperties" };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("aa", "xx");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getBrokers());
		assertEquals("t1", config.getTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getConsumerProperties());
	}

	@Test
	public void testTwoSourceProperties() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1", "--consumerGroup", "grp1", "--clientId", "cli1", 
				"--properties", "session.timeout.ms=4000,heartbeat.interval.ms=1000" };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("session.timeout.ms", "4000");
		props2.put("heartbeat.interval.ms", "1000");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getBrokers());
		assertEquals("t1", config.getTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getConsumerProperties());
	}

	@Test
	public void testTwoSourcePropertiesWithSpaces() throws ConfigurationException {
		String[] argv = { "--brokers", "xx:9092", "--topic", "t1",  "--consumerGroup", "grp1", "--clientId", "cli1", 
				"--properties", " session.timeout.ms = 4000 , heartbeat.interval.ms = 1000 " };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("session.timeout.ms", "4000");
		props2.put("heartbeat.interval.ms", "1000");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getBrokers());
		assertEquals("t1", config.getTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getConsumerProperties());
	}


}

