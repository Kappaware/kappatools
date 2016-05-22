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
package com.kappaware.kappatools.kcommon;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.JSON.Feature;
import com.kappaware.kappatools.kcommon.config.ConfigurationException;

public class Utils {
	static Logger log = LoggerFactory.getLogger(Utils.class);

	// Http definition
	private static final String HEADER_PRAGMA = "Pragma";
	private static final String HEADER_EXPIRES = "Expires";
	private static final String HEADER_CACHE_CONTROL = "Cache-Control";

	public static void setCache(HttpServletResponse response, int cacheValue) {
		if (cacheValue == 0) {
			response.setHeader(HEADER_PRAGMA, "no-cache");
			// HTTP 1.0 header
			response.setDateHeader(HEADER_EXPIRES, 1L);
			// HTTP 1.1 header: "no-cache" is the standard value,
			// "no-store" is necessary to prevent caching on FireFox.
			response.setHeader(HEADER_CACHE_CONTROL, "no-cache");
			response.addHeader(HEADER_CACHE_CONTROL, "no-store");
			response.addHeader(HEADER_CACHE_CONTROL, "must-revalidate");
			response.addHeader(HEADER_CACHE_CONTROL, "post-check=0");
			response.addHeader(HEADER_CACHE_CONTROL, "pre-check=0");

		} else {
			response.setHeader(HEADER_CACHE_CONTROL, "public");
			long now = (new Date()).getTime();
			response.setDateHeader("Date", now);
			response.setDateHeader(HEADER_EXPIRES, now + (cacheValue * 1000L));
			// HTTP 1.1 header
			String headerValue = "max-age=" + Long.toString(cacheValue);
			response.setHeader(HEADER_CACHE_CONTROL, headerValue);
		}
	}

	public static String printIsoDateTime(Long ts) {
		if (ts != null) {
			Calendar c = Calendar.getInstance();
			c.setTimeInMillis(ts);
			return DatatypeConverter.printDateTime(c);
		} else {
			return null;
		}
	}

	public static InetSocketAddress parseEndpoint(String endpoint) throws ConfigurationException {
		try {
			String[] endp = endpoint.split(":");
			if (endp.length == 2) {
				int port = Integer.parseInt(endp[1]);
				return new InetSocketAddress(endp[0], port);
			} else if (endp.length == 1) {
				int port = Integer.parseInt(endp[0]);
				return new InetSocketAddress("0.0.0.0", port);
			} else {
				throw new Exception();
			}
		} catch (Throwable t) {
			throw new ConfigurationException(String.format("Missing or invalid endpoint:%s", endpoint));
		}
	}

	public static boolean isNullOrEmpty(String s) {
		if (s == null) {
			return true;
		} else {
			return s.trim().length() == 0;
		}
	}

	// For quick and dirty debug purpose
	static JSON djson = JSON.std.with(Feature.PRETTY_PRINT_OUTPUT);

	public static String jsonPrettyString(Map<String, Object> m) {
		if (m != null) {
			try {
				return djson.asString(m);
			} catch (IOException e) {
				return "ERROR ON JSON GENERATION";
			}
		} else {
			return "null";
		}
	}
}
