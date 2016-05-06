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

package com.kappaware.kgen;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.Test;

import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kgen.config.Configuration;
import com.kappaware.kgen.config.ConfigurationImpl;
import com.kappaware.kgen.config.ParametersImpl;





public class TestConfiguration {
	static Properties sourceProperties1;
	static Properties targetProperties1;
	
	static {
		sourceProperties1 = new Properties();
		sourceProperties1.put("key.deserializer", ByteArrayDeserializer.class);
		sourceProperties1.put("value.deserializer", ByteArrayDeserializer.class);
		sourceProperties1.put("auto.offset.reset", "none");
		sourceProperties1.put("bootstrap.servers", "xx:9092");
		sourceProperties1.put("enable.auto.commit", false);
		sourceProperties1.put("group.id", "grp1");
		
		targetProperties1 = new Properties();
		targetProperties1.put("bootstrap.servers", "yy:9092");

	}


	@Test
	public void test1() throws ConfigurationException {
		String[] argv = {  "--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1" };

		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("yy:9092", config.getBrokers());
		assertEquals("t2", config.getTopic());
		assertEquals(targetProperties1, config.getProducerProperties());
		assertEquals("gen1", config.getGateId());
	}

	/*
	java.lang.AssertionError: expected:<{bootstrap.servers=yy:9092, value.serializer=class org.apache.kafka.common.serialization.ByteArraySerializer, key.serializer=class org.apache.kafka.common.serialization.ByteArraySerializer}> 
	but was:<{bootstrap.servers=yy:9092}>
	*/

	
	
	@Test
	public void testMandatoryTargetBroker() throws ConfigurationException {
		String[] argv = { "--topic", "t2", "--gateId", "gen1"};
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertTrue(e.getMessage().contains("Missing required option(s) [brokers]"));
			return;
		}
		fail("Missing target broker undetected");
	}

	// ------------------------------------------ Target properties
	@Test
	public void testInvalidTargetProperties1() throws ConfigurationException {
		String[] argv = {"--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1", "--properties", "aa=xx" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid target property 'aa'!", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}

	@Test
	public void testInvalidTargetProperties2() throws ConfigurationException {
		String[] argv = {"--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1", "--properties", "aa" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Target property must be as name=value. Found 'aa'", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}

	@Test
	public void testInvalidTargetProperties3() throws ConfigurationException {
		String[] argv = { "--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1", "--properties", "aa=" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Target property must be as name=value. Found 'aa='", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}
	
	@Test
	public void testInvalidTargetProperties4() throws ConfigurationException {
		String[] argv = { "--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1",  "--properties", "=xx" };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid target property ''!", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}
	
	
	@Test
	public void testEmptyTargetProperties5() throws ConfigurationException {
		String[] argv = { "--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1",  "--properties", "" };
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("yy:9092", config.getBrokers());
		assertEquals("t2", config.getTopic());
		assertEquals(targetProperties1, config.getProducerProperties());
	}
	

	@Test
	public void testForceInvalidTargetProperties() throws ConfigurationException {
		String[] argv = {  "--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1", "--properties", "aa=xx", "--forceProperties" };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("aa", "xx");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("yy:9092", config.getBrokers());
		assertEquals("t2", config.getTopic());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetProperties() throws ConfigurationException {
		String[] argv = { "--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1", 
				"--properties", "acks=all,client.id=toto" };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("yy:9092", config.getBrokers());
		assertEquals("t2", config.getTopic());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetPropertiesWithSpaces() throws ConfigurationException {
		String[] argv = {  "--brokers", "yy:9092", "--topic", "t2", "--gateId", "gen1",
				"--properties", " acks = all , client.id = toto " };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("yy:9092", config.getBrokers());
		assertEquals("t2", config.getTopic());
		assertEquals(props2, config.getProducerProperties());
	}

	
}

