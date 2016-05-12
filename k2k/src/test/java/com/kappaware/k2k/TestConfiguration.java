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
package com.kappaware.k2k;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;

import com.kappaware.k2k.config.Configuration;
import com.kappaware.k2k.config.ConfigurationException;
import com.kappaware.k2k.config.ConfigurationImpl;
import com.kappaware.k2k.config.ParametersImpl;




public class TestConfiguration {
	static Properties sourceProperties1;
	static Properties targetProperties1;
	
	static {
		sourceProperties1 = new Properties();
		sourceProperties1.put("auto.offset.reset", "latest");
		sourceProperties1.put("bootstrap.servers", "xx:9092");
		sourceProperties1.put("enable.auto.commit", true);
		sourceProperties1.put("group.id", "grp1");
		sourceProperties1.put("client.id", "client1");
		sourceProperties1.put("heartbeat.interval.ms", "2500");
		sourceProperties1.put("session.timeout.ms", "10000");
		
		targetProperties1 = new Properties();
		targetProperties1.put("bootstrap.servers", "yy:9092");
		sourceProperties1.put("client.id", "client1");

	}


	@Test
	public void test1() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--clientId", "client1" };

		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(sourceProperties1, config.getConsumerProperties());
		assertEquals(targetProperties1, config.getProducerProperties());
	}

	@Test
	public void testMandatorySourceBroker() throws ConfigurationException {
		String[] argv = { "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--clientId", "client1"  };
		try {
			new ConfigurationImpl(new ParametersImpl(argv));
		} catch (ConfigurationException e) {
			assertTrue(e.getMessage().contains("Missing required option(s) [sourceBrokers]"));
			return;
		}
		fail("Missing source broker undetected");
	}
	// ------------------------------------------ Source properties
	@Test
	public void testInvalidSourceProperties1() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa=xx", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa=", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "=xx", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "", "--clientId", "client1"  };
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(sourceProperties1, config.getConsumerProperties());
	}
	

	@Test
	public void testForceInvalidSourceProperties() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa=xx", "--forceProperties", "--clientId", "client1"  };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("aa", "xx");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getConsumerProperties());
	}

	@Test
	public void testTwoSourceProperties() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", 
				"--sourceProperties", "metrics.sample.window.ms=20000,metrics.num.samples=4", "--clientId", "client1"  };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("metrics.sample.window.ms", "20000");
		props2.put("metrics.num.samples", "4");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getConsumerProperties());
	}

	@Test
	public void testTwoSourcePropertiesWithSpaces() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", 
				"--sourceProperties", " metrics.sample.window.ms = 20000 , metrics.num.samples = 4 ", "--clientId", "client1"  };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("metrics.sample.window.ms", "20000");
		props2.put("metrics.num.samples", "4");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getConsumerProperties());
	}

	// ------------------------------------------ Target properties
	@Test
	public void testInvalidTargetProperties1() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa=xx", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa=", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "=xx", "--clientId", "client1"  };
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
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "", "--clientId", "client1"  };
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(targetProperties1, config.getProducerProperties());
	}
	

	@Test
	public void testForceInvalidTargetProperties() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa=xx", "--forceProperties" , "--clientId", "client1" };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("aa", "xx");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetProperties() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", 
				"--targetProperties", "acks=all,compression.type=gzip", "--clientId", "client1"  };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("compression.type", "gzip");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetPropertiesWithSpaces() throws ConfigurationException {
		String[] argv = { "--sourceBrokers", "xx:9092", "--sourceTopic", "t1", "--targetBrokers", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", 
				"--targetProperties", " acks = all , compression.type = gzip ", "--clientId", "client1"  };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("compression.type", "gzip");
		
		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getProducerProperties());
	}

	
}

