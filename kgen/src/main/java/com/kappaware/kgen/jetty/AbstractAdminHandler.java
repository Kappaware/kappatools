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

package com.kappaware.kgen.jetty;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.JSONObjectException;
import com.kappaware.kgen.Utils;
import com.kappaware.kgen.config.ConfigurationException;

public abstract class AbstractAdminHandler extends AbstractHandler {
	private IpMatcherImpl ipMatcher;
	private JSON json = JSON.std.with(JSON.Feature.PRETTY_PRINT_OUTPUT);

	public AbstractAdminHandler(String adminAllowedNetwork) throws ConfigurationException {

		String[] segments = adminAllowedNetwork.split(",");
		this.ipMatcher = new IpMatcherImpl();
		for (String segmentDef : segments) {
			this.ipMatcher.addSegment(segmentDef);
		}
	}

	public abstract Object handleRequest(HttpServletRequest request, HttpServletResponse response) throws HttpServerException;

	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		try {
			if (!this.ipMatcher.match(request.getRemoteAddr())) {
				throw new HttpServerException(HttpServletResponse.SC_FORBIDDEN, String.format("Request from %s are not allowed", request.getRemoteAddr()));
			}
			Object o = this.handleRequest(request, response);
			if (o != null) {
				try {
					String jsonResponse;
					synchronized (o) {
						jsonResponse = json.asString(o);
					}
					response.setContentType("application/json;charset=UTF-8");
					response.setStatus(HttpServletResponse.SC_OK);
					Utils.setCache(response, 0);
					PrintWriter w = response.getWriter();
					w.print(jsonResponse);
					w.flush();
					w.close();
				} catch (JSONObjectException e) {
					throw new HttpServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unable to generate a JSON string:" + e.getMessage());
				}
			} else {
				throw new HttpServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Null json object");
			}
		} catch (HttpServerException hse) {
			response.sendError(hse.getErrorCode(), hse.getMessage());
		}
		baseRequest.setHandled(true);
	}
}
