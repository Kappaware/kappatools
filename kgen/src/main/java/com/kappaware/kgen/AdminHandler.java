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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.kappaware.kgen.config.ConfigurationException;
import com.kappaware.kgen.jetty.AbstractAdminHandler;
import com.kappaware.kgen.jetty.HttpServerException;

public class AdminHandler extends AbstractAdminHandler {
	private Engine engine;

	public AdminHandler(String adminAllowedNetwork, Engine engine) throws ConfigurationException {
		super(adminAllowedNetwork);
		this.engine = engine;
	}

	@Override
	public Object handleRequest(HttpServletRequest request, HttpServletResponse response) throws HttpServerException {
		if(request.getMethod().equals("GET")) {
			String pathInfo = request.getPathInfo();
			if(pathInfo.equalsIgnoreCase("/stats")) {
				return engine.getStats();
			} else {
				throw new HttpServerException(HttpServletResponse.SC_NOT_FOUND, String.format("Invalid path info:'%s'", pathInfo));
			}
		} else {
			throw new HttpServerException(HttpServletResponse.SC_NOT_FOUND, "Only GET method handled here!");
		}
	}

}
