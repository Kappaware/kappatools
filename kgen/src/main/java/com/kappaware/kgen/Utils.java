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

import java.util.Date;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
	static Logger log = LoggerFactory.getLogger(Engine.class);

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

	
}
