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

package com.kappaware.kappatools.kcommon.jetty;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.config.Settings;

public class AdminHandler extends AbstractAdminHandler {
	private Engine engine;

	public AdminHandler(String adminAllowedNetwork, Engine engine) throws ConfigurationException {
		super(adminAllowedNetwork);
		this.engine = engine;
	}

	@Override
	public Result handleRequest(HttpServletRequest request, HttpServletResponse response) throws HttpServerException {
		if (request.getMethod().equals("GET")) {
			String pathInfo = request.getPathInfo();
			if (pathInfo.equalsIgnoreCase("/stats")) {
				return new Result(HttpServletResponse.SC_OK, engine.getStats());
			} else if (pathInfo.equalsIgnoreCase("/settings")) {
				return new Result(HttpServletResponse.SC_OK, engine.getSettings());
			} else {
				throw new HttpServerException(HttpServletResponse.SC_NOT_FOUND, String.format("Invalid path info:'%s'", pathInfo));
			}
		} else if (request.getMethod().equals("PUT")) {
			String pathInfo = request.getPathInfo();
			if (pathInfo.equalsIgnoreCase("/settings")) {
				try {
					Settings settings = this.getJson().beanFrom(engine.getSettings().getClass(), request.getReader());
					engine.getSettings().apply(settings);
					return new Result(HttpServletResponse.SC_NO_CONTENT, null);
				} catch (IOException e) {
					throw new HttpServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, String.format("Server error:'%s'", e.getMessage()));
				}
			} else {
				throw new HttpServerException(HttpServletResponse.SC_NOT_FOUND, String.format("Invalid path info:'%s'", pathInfo));
			}
		} else {
			throw new HttpServerException(HttpServletResponse.SC_NOT_FOUND, "Only GET method handled here!");
		}
	}
	
}
