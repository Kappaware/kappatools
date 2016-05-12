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

import java.io.IOException;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jr.ob.JSON;
import com.kappaware.kappatools.kcommon.ExtTs;
import com.kappaware.kappatools.kcommon.ExtTsFactory;
import com.kappaware.kappatools.kcommon.HeaderBuilder;
import com.kappaware.king.config.Configuration;


@SuppressWarnings("serial")
public class KingServlet extends HttpServlet {
	static Logger log = LoggerFactory.getLogger(KingServlet.class);

	private static final int ASYNC_TIMEOUT_MS = 10136000;

	private ExtTsFactory extTsFactory;
	private HeaderBuilder headerBuilder;
	private Configuration config;
	private KafkaProducer<String, byte[]> producer;
	private JSON json;
	private EngineImpl engine;

	public KingServlet(Configuration config, EngineImpl engine) {
		this.config = config;
		this.engine = engine;
		this.extTsFactory = new ExtTsFactory(config.getGateId());
		this.headerBuilder = new HeaderBuilder();
		this.producer = new KafkaProducer<String, byte[]>(config.getProducerProperties(),  new StringSerializer(), new ByteArraySerializer());
		engine.init(producer.partitionsFor(this.config.getTopic()));
		this.json = JSON.std.without(JSON.Feature.PRETTY_PRINT_OUTPUT);

		// Just to ensure we are able to access target topic.
		this.producer.partitionsFor(this.config.getTopic()).size();
		
	}

	@Override
	public void destroy() {
		this.producer.flush();
		this.producer.close();
	}

	static class RequestContext {
		HttpServletRequest request;
		HttpServletResponse response;
		ExtTs extTs;
		ReadBuffer readBuffer;
		AsyncContext asyncContext;

		public RequestContext(HttpServletRequest request, HttpServletResponse response) {
			this.request = request;
			this.response = response;
		}
	}

	// https://weblogs.java.net/blog/swchan2/archive/2013/04/16/non-blocking-io-servlet-31-example (/src/doc/java)
	// http://developerlife.com/tutorials/?p=1437

	@Override
	protected void service(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
		log.trace("Entering service");
		RequestContext requestContext = new RequestContext(request, response);

		// Need to perform this asap, to have the most accurate timestamp
		requestContext.extTs = this.extTsFactory.get();

		// Abort immediately if not allowed
		if (!this.config.getAllowedNetworkMatcher().match(request.getRemoteAddr())) {
			response.sendError(HttpServletResponse.SC_FORBIDDEN, String.format("Request from %s are not allowed", request.getRemoteAddr()));
			return;
		}
		requestContext.readBuffer = new ReadBuffer(request.getContentLengthLong(), config);
		requestContext.asyncContext = request.startAsync();
		requestContext.asyncContext.setTimeout(ASYNC_TIMEOUT_MS);

		requestContext.asyncContext.addListener(new AsyncListener() {

			@Override
			public void onComplete(AsyncEvent event) throws IOException {
				log.trace("Entering onComplete()");
				// Nothing to do, as handled by the readListener
			}

			@Override
			public void onTimeout(AsyncEvent event) throws IOException {
				log.error(String.format("From %s:%s -> Timeout on Http Async processing", request.getRemoteAddr(), request.getRequestURL()));
				handleComplete(requestContext, "Timeout on Http Async processing");
			}

			@Override
			public void onError(AsyncEvent event) throws IOException {
				log.error(String.format("From %s:%s -> Error on Http Async processing", request.getRemoteAddr(), request.getRequestURL()), event.getThrowable());
				handleComplete(requestContext, String.format("Error on Http Async processing: %s", event.getThrowable().getMessage()));
			}

			@Override
			public void onStartAsync(AsyncEvent event) throws IOException {
				// This is never called, as the listener os set AFTER the startAsync()			}
				log.trace("Entering onStartAsync()");
			}
		});

		log.trace("Will set ReadListener");
		final ServletInputStream input = request.getInputStream();
		byte[] stepBuffer = new byte[config.getReadStep()];
		input.setReadListener(new ReadListener() {

			@Override
			public void onDataAvailable() throws IOException {
				int len = -1;
				while (input.isReady() && (len = input.read(stepBuffer)) != -1) {
					requestContext.readBuffer.add(stepBuffer, len);
				}
			}

			@Override
			public void onAllDataRead() throws IOException {
				log.trace("Entering onAllDataRead()");
				handleComplete(requestContext, null);
			}

			@Override
			public void onError(Throwable t) {
				log.error(String.format("From %s:%s -> Error on Http Read processing", request.getRemoteAddr(), request.getRequestURL()), t);
				handleComplete(requestContext, String.format("Error on Http Read processing", t.getMessage()));
			}

		});
	}

	abstract class MyCallback implements Callback {
		ProducerRecord<String, byte[]> record;
		MyCallback(ProducerRecord<String, byte[]> record) {
			this.record = record;
		}
	}
	
	/*
	 * HandleComplete should not generate an exception, otherwise, this will be trapped in an 'onError()' callback which will call handleComplete() again 
	 */
	void handleComplete(RequestContext requestContext, String error) {
		try {
			log.trace("Entering handleComplete");
			Key key = new Key(requestContext.request, requestContext.extTs, this.headerBuilder, this.config.getKeyLevel());
			if (requestContext.readBuffer.isOverflow()) {
				key.setTruncated(true);
			}
			if (error != null) {
				key.addError(error);
			}
			log.trace(String.format("Will lookup partition count for topic %s", this.config.getTopic()));
			int partitionCount = this.producer.partitionsFor(this.config.getTopic()).size();
			log.trace(String.format("Partition count:%d", partitionCount));
			int partition;
			if (key.getPartitionKey() != null) {
				partition = Utils.abs(Utils.murmur2(key.getPartitionKey().getBytes())) % partitionCount;
			} else {
				partition = (int) (Math.random() * partitionCount);
			}
			log.trace(String.format("Pushing message to kafka to partition %d", partition));
			String keyString;
			try {
				keyString = json.asString(key);
			} catch (IOException e) {
				throw new RuntimeException(String.format("Unable to generate a json string from %s (class:%s) -> %s", key.toString(), key.getClass().getName(), e));
			}
			ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>(this.config.getTopic(), partition, keyString, requestContext.readBuffer.getContent());
			this.producer.send(producerRecord, new MyCallback(producerRecord) {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					log.trace("producer.send() onCompletion() callcack");
					if (e != null) {
						log.error(String.format("From %s:%s -> Error on Kafka send", requestContext.request.getRemoteAddr(), requestContext.request.getRequestURL()), e);
					} else {
						log.debug(String.format("Succefuly send message on partition #%d", partition));
					}
					engine.addToStats(this.record.key().getBytes(), metadata.partition(), metadata.offset());
					if (engine.isMesson()) {
						log.info(String.format("part:offset = %d:%d, key = '%s', value = '%s'", metadata.partition(), metadata.offset(), record.key(), new String(record.value())));
					}
					requestContext.response.setStatus(HttpServletResponse.SC_OK);
					requestContext.asyncContext.complete();
				}
			});
		} catch (Throwable e) {
			log.error("Error in handleComplete()", e);
			requestContext.response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			requestContext.asyncContext.complete();
		}
	}
}
