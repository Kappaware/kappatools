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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.JSONObjectException;
import com.kappaware.kappatools.kcommon.ExtTsFactory;

import junit.framework.TestCase;

public class TestJsonKey extends TestCase {

	static class JsonSerializer<T> {
		private JSON json;

		public JsonSerializer() {
			json = JSON.std.without(JSON.Feature.PRETTY_PRINT_OUTPUT);
		}

		public String serialize( T data) {
			try {
				return json.asString(data);
			} catch (IOException e) {
				throw new RuntimeException(String.format("Unable to generate a json string from %s (class:%s) -> %s", data.toString(), data.getClass().getName(), e));
			}
		}
	}

	private JsonSerializer<Key> jsonSerializer;
	private ExtTsFactory factory;
	private JSON json;

	@Override
	protected void setUp() {
		this.jsonSerializer = new JsonSerializer<Key>();
		factory = new ExtTsFactory("g1");
		this.json = JSON.std;
	}

	@Override
	protected void tearDown() {
		this.jsonSerializer = null;
	}

	public void test1() throws JSONObjectException, IOException {
		Key key = new Key();
		String blob = this.jsonSerializer.serialize(key);
		Object o = this.json.beanFrom(Key.class, blob);
		assertTrue(o instanceof Key);
		Key k = (Key) o;
		assertNull(k.getExtTs());
		assertNull(k.getContentType());
		assertNull(k.getRemoteAddr());
		assertNull(k.getErrors());
		assertNull(k.getTruncated());
	}

	public void test2() throws JSONObjectException, IOException {
		Key key = new Key();
		key.setExtTs(factory.get());

		key.setVerb("POST");
		key.setCharacterEncoding("UTF-8");
		key.setContentLength(100L);
		key.setContentType("text/json");
		key.setProtocol("HTTP/1.1");
		key.setRemoteAddr("10.0.0.1");
		key.setScheme("http");
		key.setServerName("my.server.com");
		key.setServerPort(8088);
		key.setPathInfo("/xx/yy/zz");
		key.setTruncated(true);

		Map<String, List<String>> params = new HashMap<String, List<String>>();
		params.put("param1", Arrays.asList(new String[] { "v10" }));
		params.put("param2", Arrays.asList(new String[] { "v20", "v21", "v22" }));
		key.setParameters(params);

		Map<String, List<String>> headers = new HashMap<String, List<String>>();
		headers.put("header1", Arrays.asList(new String[] { "h10" }));
		headers.put("header2", Arrays.asList(new String[] { "h20", "h21", "h22" }));
		key.setHeaders(headers);

		key.addError("error1");
		key.addError("error2");

		String blob = this.jsonSerializer.serialize(key);
		Object o = this.json.beanFrom(Key.class, blob);
		assertTrue(o instanceof Key);
		Key k = (Key) o;

		assertEquals("g1", k.getExtTs().getGateId());
		assertEquals(0, k.getExtTs().getCounter());

		assertEquals("POST", key.getVerb());
		assertEquals("UTF-8", key.getCharacterEncoding());
		assertEquals((Long) 100L, key.getContentLength());
		assertEquals("text/json", key.getContentType());
		assertEquals("HTTP/1.1", key.getProtocol());
		assertEquals("10.0.0.1", key.getRemoteAddr());
		assertEquals("http", key.getScheme());
		assertEquals("my.server.com", key.getServerName());
		assertEquals((Integer) 8088, key.getServerPort());
		assertEquals("/xx/yy/zz", key.getPathInfo());

		assertEquals(params, k.getParameters());
		assertEquals(headers, k.getHeaders());
		assertTrue(k.getTruncated());

		assertEquals(Arrays.asList(new String[] { "error1", "error2" }), key.getErrors());

	}
}
