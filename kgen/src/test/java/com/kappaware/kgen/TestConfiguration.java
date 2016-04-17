package com.kappaware.kgen;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import com.kappaware.kgen.config.Configuration;
import com.kappaware.kgen.config.ConfigurationException;
import com.kappaware.kgen.config.ConfigurationImpl;
import com.kappaware.kgen.config.Parameters;





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
		targetProperties1.put("key.serializer", ByteArraySerializer.class);
		targetProperties1.put("value.serializer", ByteArraySerializer.class);

	}


	@Test
	public void test1() throws ConfigurationException {
		String[] argv = {  "--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1" };

		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t2", config.getTargetTopic());
		assertEquals(targetProperties1, config.getProducerProperties());
		assertEquals("gen1", config.getGateId());
	}

	@Test
	public void testMandatoryTargetBroker() throws ConfigurationException {
		String[] argv = { "--targetTopic", "t2", "--gateId", "gen1"};
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertTrue(e.getMessage().contains("Missing required option(s) [targetBrokers]"));
			return;
		}
		fail("Missing target broker undetected");
	}

	// ------------------------------------------ Target properties
	@Test
	public void testInvalidTargetProperties1() throws ConfigurationException {
		String[] argv = {"--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1", "--targetProperties", "aa=xx" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid target property 'aa'!", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}

	@Test
	public void testInvalidTargetProperties2() throws ConfigurationException {
		String[] argv = {"--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1", "--targetProperties", "aa" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Target property must be as name=value. Found 'aa'", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}

	@Test
	public void testInvalidTargetProperties3() throws ConfigurationException {
		String[] argv = { "--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1", "--targetProperties", "aa=" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Target property must be as name=value. Found 'aa='", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}
	
	@Test
	public void testInvalidTargetProperties4() throws ConfigurationException {
		String[] argv = { "--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1",  "--targetProperties", "=xx" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid target property ''!", e.getMessage());
			return;
		}
		fail("invalid target property undetected");
	}
	
	
	@Test
	public void testEmptyTargetProperties5() throws ConfigurationException {
		String[] argv = { "--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1",  "--targetProperties", "" };
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t2", config.getTargetTopic());
		assertEquals(targetProperties1, config.getProducerProperties());
	}
	

	@Test
	public void testForceInvalidTargetProperties() throws ConfigurationException {
		String[] argv = {  "--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1", "--targetProperties", "aa=xx", "--forceProperties" };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("aa", "xx");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t2", config.getTargetTopic());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetProperties() throws ConfigurationException {
		String[] argv = { "--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1", 
				"--targetProperties", "acks=all,client.id=toto" };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t2", config.getTargetTopic());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetPropertiesWithSpaces() throws ConfigurationException {
		String[] argv = {  "--targetBroker", "yy:9092", "--targetTopic", "t2", "--gateId", "gen1",
				"--targetProperties", " acks = all , client.id = toto " };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t2", config.getTargetTopic());
		assertEquals(props2, config.getProducerProperties());
	}

	
}

