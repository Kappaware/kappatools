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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jr.ob.JSON;
import com.kappaware.king.config.Configuration;
import com.kappaware.king.config.ConfigurationImpl;
import com.kappaware.king.config.ParametersImpl;

import info.batey.kafka.unit.KafkaUnit;

public class TestIntegration1 {
	static Logger log = LoggerFactory.getLogger(TestIntegration1.class);

	static final int KAFKA_PORT = 5000;
	static final int ZK_PORT = 5001;
	static final int WHARF_PORT = 5002;
	static final String TOPIC = "topic1";
	static final int PARTITIONS_COUNT = 5;
	static KafkaUnit kafkaUnit;
	static String brokerList;
	static String endpoint;
	static JSON json;

	@BeforeClass
	public static void setUpClass() {
		brokerList = String.format("localhost:%d", KAFKA_PORT);
		endpoint = String.format("0.0.0.0:%d", WHARF_PORT);
		kafkaUnit = new KafkaUnit(ZK_PORT, KAFKA_PORT);
		kafkaUnit.startup();
		kafkaUnit.createTopic(TOPIC, PARTITIONS_COUNT);
		json = JSON.std;
	}

	@AfterClass
	public static void tearDownClass() {
		kafkaUnit.shutdown();
	}

	HttpClient newHttpClient() throws Exception {
		HttpClient httpClient = new HttpClient();
		httpClient.setFollowRedirects(false);
		httpClient.setMaxConnectionsPerDestination(1); // Need this, otherwhise, message ordering will not be preserved
		httpClient.setStrictEventOrdering(true);
		httpClient.start();
		return httpClient;
	}

	void closeHttpClient(HttpClient httpClient) throws Exception {
		httpClient.stop();
	}

	KafkaConsumer<String, String> newConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(TOPIC));
		return consumer;
	}
	
	void closeConsumer(KafkaConsumer<String,String> consumer) {
		consumer.close();
	}

	void send(HttpClient httpClient, String target, String data) throws InterruptedException, TimeoutException, ExecutionException {
		Request request = httpClient.newRequest(target);
		request.method(HttpMethod.PUT).agent("Test Java client").content(new StringContentProvider(data), "text/plain");
		ContentResponse response = request.send();
		if (response.getStatus() != 200) {
			throw new RuntimeException("Invalid status");
		}
	}

	
	interface RecordChecker {
		void checkRecord(ConsumerRecord<String, String> record, Key key, int cnt);
	}
	
	private void test(String[] argv, String target, String[] dataSet, RecordChecker recordChecker) throws Exception {

		KafkaConsumer<String, String> consumer = this.newConsumer();
		consumer.poll(0);  // To force partition assignment and sync to last pos

		Configuration config = new ConfigurationImpl(new ParametersImpl(argv));
		Server server = Main.buildMainServer(config, new EngineImpl(config));
		server.start();
		HttpClient httpClient = newHttpClient();

		for(String data : dataSet) {
			this.send(httpClient, target, data);
		}

		int cnt = 0;
		Integer partition = null;
		while (cnt < dataSet.length) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			assertTrue(records.count() > 0);
			for (ConsumerRecord<String, String> record : records) {
				if(partition == null) {
					partition = record.partition();
				}
				assertEquals((int)partition, record.partition());
				Object o = json.beanFrom(Key.class, record.key());
				assertTrue(o instanceof Key);
				recordChecker.checkRecord(record, (Key)o, cnt);
				cnt++;
			}
		}
		ConsumerRecords<String, String> records = consumer.poll(0);
		assertEquals(0, records.count());

		this.closeHttpClient(httpClient);
		this.closeConsumer(consumer);
		server.stop();
	}
	
	@Test
	public void testLevel1() throws Exception {
		String[] argv = { "--brokers", brokerList, "--topic", TOPIC, "--gateId", "gate1", "--endpoint", endpoint, "--keyLevel", "1" };
		String[] dataSet = { "aaalkl", "xxxxxxx", "88 88 00 99", "cvcvcvcvc", "" };
		String target = String.format("http://127.0.0.1:%d/main/xx?p1=aa&p2=bb&p2=cc", WHARF_PORT);
		
		this.test(argv, target, dataSet, new RecordChecker() {
			@Override
			public void checkRecord(ConsumerRecord<String, String> record, Key key, int cnt) {
				//System.out.printf(" ----- cnt = %d  partition = %d, offset = %d, key = %s, value = %s\n", cnt, record.partition(), record.offset(), record.key(), record.value());
				assertEquals(dataSet[cnt], record.value());

				assertEquals("gate1", key.getExtTs().getGateId());
				assertEquals("127.0.0.1", key.getPartitionKey());
				assertEquals(null, key.getVerb());
				assertEquals(null, key.getCharacterEncoding());
				assertEquals(null, key.getContentLength());
				assertEquals(null, key.getContentType());
				assertEquals(null, key.getProtocol());
				assertEquals(null, key.getRemoteAddr());
				assertEquals(null, key.getScheme());
				assertEquals(null, key.getServerName());
				assertEquals(null, key.getServerPort());
				assertEquals(null, key.getPathInfo());
				assertEquals(null, key.getParameters());
				assertEquals(null, key.getHeaders());
				assertNull(key.getTruncated());
			}
		});
	}

	
	@Test
	public void testLevel2() throws Exception {
		String[] argv = { "--brokers", brokerList, "--topic", TOPIC, "--gateId", "gate1", "--endpoint", endpoint, "--keyLevel", "2" };
		String[] dataSet = { "xxxxxxx", "zzzzz", "88 188 00 99", "" };
		String target = String.format("http://127.0.0.1:%d/main/xx?p1=aa&p2=bb&p2=cc", WHARF_PORT);
		
		this.test(argv, target, dataSet, new RecordChecker() {
			@Override
			public void checkRecord(ConsumerRecord<String, String> record, Key key, int cnt) {
				//System.out.printf(" ----- cnt = %d  partition = %d, offset = %d, key = %s, value = %s\n", cnt, record.partition(), record.offset(), record.key(), record.value());
				assertEquals(dataSet[cnt], record.value());

				assertEquals("gate1", key.getExtTs().getGateId());
				assertEquals("127.0.0.1", key.getPartitionKey());
				assertEquals("PUT", key.getVerb());
				assertEquals(null, key.getCharacterEncoding());
				assertEquals(new Long((dataSet[cnt]).length()), key.getContentLength());
				assertEquals("text/plain", key.getContentType());
				assertEquals(null, key.getProtocol());
				assertEquals("127.0.0.1", key.getRemoteAddr());
				assertEquals(null, key.getScheme());
				assertEquals(null, key.getServerName());
				assertEquals(null, key.getServerPort());
				assertEquals("/main/xx", key.getPathInfo());
				assertEquals(null, key.getParameters());
				assertEquals(null, key.getHeaders());
				assertNull(key.getTruncated());
			}
		});
	}

	
	@Test
	public void testLevel3() throws Exception {
		String[] argv = { "--brokers", brokerList, "--topic", TOPIC, "--gateId", "gate1", "--endpoint", endpoint, "--keyLevel", "3" };
		String[] dataSet = { "xxxxxxx", "zzzzz", "88 188 00 99", "" };
		String target = String.format("http://127.0.0.1:%d/main/xx?p1=aa&p2=bb&p2=cc", WHARF_PORT);
		
		this.test(argv, target, dataSet, new RecordChecker() {
			@Override
			public void checkRecord(ConsumerRecord<String, String> record, Key key, int cnt) {
				//System.out.printf(" ----- cnt = %d  partition = %d, offset = %d, key = %s, value = %s\n", cnt, record.partition(), record.offset(), record.key(), record.value());
				assertEquals(dataSet[cnt], record.value());


				Map<String, List<String>> params = new  HashMap<String, List<String>>();
				params.put("p1", Arrays.asList(  new String[] {"aa"} ));
				params.put("p2", Arrays.asList(  new String[] {"bb", "cc"} ));

				Map<String, List<String>> headers = new  HashMap<String, List<String>>();
				headers.put("User-Agent", Arrays.asList(  new String[] {"Test Java client"} ));
				headers.put("Host", Arrays.asList(  new String[] {"127.0.0.1:5002"} ));
				headers.put("Accept-Encoding", Arrays.asList(  new String[] {"gzip"} ));
				headers.put("Content-Length", Arrays.asList(  new String[] { Integer.toString(dataSet[cnt].length()) } ));
				headers.put("Content-Type", Arrays.asList(  new String[] {"text/plain"} ));
				
				assertEquals("gate1", key.getExtTs().getGateId());
				assertEquals("127.0.0.1", key.getPartitionKey());
				assertEquals("PUT", key.getVerb());
				assertEquals(null, key.getCharacterEncoding());
				assertEquals(new Long((dataSet[cnt]).length()), key.getContentLength());
				assertEquals("text/plain", key.getContentType());
				assertEquals("HTTP/1.1", key.getProtocol());
				assertEquals("127.0.0.1", key.getRemoteAddr());
				assertEquals("http", key.getScheme());
				assertEquals("127.0.0.1", key.getServerName());
				assertEquals((Integer)WHARF_PORT, key.getServerPort());
				assertEquals("/main/xx", key.getPathInfo());
				assertEquals(params, key.getParameters());
				assertEquals(headers, key.getHeaders());
				assertNull(key.getTruncated());
			}
		});
	}
	
	// Un-commissioned as maxMessageSize has been removed from options
	public void testTruncate() throws Exception {
		String[] argv = { "--brokers", brokerList, "--topic", TOPIC, "--gateId", "gate1", "--endpoint", endpoint, "--keyLevel", "1", "--maxMessageSize", "10" };
		String[] dataSet = { "aaalkl", "0123456789012", "0123456789" };
		String target = String.format("http://127.0.0.1:%d/main/xx?p1=aa&p2=bb&p2=cc", WHARF_PORT);
		
		this.test(argv, target, dataSet, new RecordChecker() {
			@Override
			public void checkRecord(ConsumerRecord<String, String> record, Key key, int cnt) {
				System.out.printf(" ----- cnt = %d  partition = %d, offset = %d, key = %s, value = %s\n", cnt, record.partition(), record.offset(), record.key(), record.value());
				String ref = dataSet[cnt];
				if(ref.length() > 10) {
					ref = ref.substring(0, 10);
				}
				assertEquals(ref, record.value());

				assertEquals("gate1", key.getExtTs().getGateId());
				assertEquals("127.0.0.1", key.getPartitionKey());
				assertEquals(null, key.getVerb());
				assertEquals(null, key.getCharacterEncoding());
				assertEquals(null, key.getContentLength());
				assertEquals(null, key.getContentType());
				assertEquals(null, key.getProtocol());
				assertEquals("127.0.0.1", key.getRemoteAddr());
				assertEquals(null, key.getScheme());
				assertEquals(null, key.getServerName());
				assertEquals(null, key.getServerPort());
				assertEquals(null, key.getPathInfo());
				assertEquals(null, key.getParameters());
				assertEquals(null, key.getHeaders());
				if( dataSet[cnt].length() > 10) {
					assertTrue(key.getTruncated());
				} else {
					assertNull(key.getTruncated());
				}
			}
		});
	}


}
