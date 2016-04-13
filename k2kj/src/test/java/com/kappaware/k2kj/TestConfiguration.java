package com.kappaware.k2kj;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import com.kappaware.k2kj.config.Configuration;
import com.kappaware.k2kj.config.ConfigurationException;
import com.kappaware.k2kj.config.ConfigurationImpl;
import com.kappaware.k2kj.config.Parameters;




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
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1" };

		Configuration config = new ConfigurationImpl(new Parameters(argv));
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
		String[] argv = { "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertTrue(e.getMessage().contains("Missing required option(s) [sourceBrokers]"));
			return;
		}
		fail("Missing source broker undetected");
	}
	// ------------------------------------------ Source properties
	@Test
	public void testInvalidSourceProperties1() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa=xx" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid source property 'aa'!", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}

	@Test
	public void testInvalidSourceProperties2() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Source property must be as name=value. Found 'aa'", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}

	@Test
	public void testInvalidSourceProperties3() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa=" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Source property must be as name=value. Found 'aa='", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}
	
	@Test
	public void testInvalidSourceProperties4() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "=xx" };
		try {
			new ConfigurationImpl(new Parameters(argv));
		} catch (ConfigurationException e) {
			assertEquals("Invalid source property ''!", e.getMessage());
			return;
		}
		fail("invalid source property undetected");
	}
	
	
	@Test
	public void testEmptySourceProperties5() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "" };
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(sourceProperties1, config.getConsumerProperties());
	}
	

	@Test
	public void testForceInvalidSourceProperties() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--sourceProperties", "aa=xx", "--forceProperties" };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("aa", "xx");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getConsumerProperties());
	}

	@Test
	public void testTwoSourceProperties() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", 
				"--sourceProperties", "session.timeout.ms=10000,client.id=toto" };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("session.timeout.ms", "10000");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
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
				"--sourceProperties", " session.timeout.ms = 10000 , client.id = toto " };

		Properties props2 = (Properties) sourceProperties1.clone(); 
		props2.put("session.timeout.ms", "10000");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
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
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa=xx" };
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
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa" };
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
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa=" };
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
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "=xx" };
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
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "" };
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(targetProperties1, config.getProducerProperties());
	}
	

	@Test
	public void testForceInvalidTargetProperties() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", "--targetProperties", "aa=xx", "--forceProperties" };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("aa", "xx");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetProperties() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", 
				"--targetProperties", "acks=all,client.id=toto" };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getProducerProperties());
	}

	@Test
	public void testTwoTargetPropertiesWithSpaces() throws ConfigurationException {
		String[] argv = { "--sourceBroker", "xx:9092", "--sourceTopic", "t1", "--targetBroker", "yy:9092", "--targetTopic", "t2", "--consumerGroup", "grp1", 
				"--targetProperties", " acks = all , client.id = toto " };

		Properties props2 = (Properties) targetProperties1.clone(); 
		props2.put("acks", "all");
		props2.put("client.id", "toto");
		
		Configuration config = new ConfigurationImpl(new Parameters(argv));
		assertEquals("xx:9092", config.getSourceBrokers());
		assertEquals("yy:9092", config.getTargetBrokers());
		assertEquals("t1", config.getSourceTopic());
		assertEquals("t2", config.getTargetTopic());
		assertEquals("grp1", config.getConsumerGroup());
		assertEquals(props2, config.getProducerProperties());
	}

	
}

