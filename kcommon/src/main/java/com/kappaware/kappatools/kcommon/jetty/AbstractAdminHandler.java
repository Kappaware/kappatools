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
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.fasterxml.jackson.jr.ob.JSON;
import com.fasterxml.jackson.jr.ob.JSONObjectException;
import com.kappaware.kappatools.kcommon.Utils;
import com.kappaware.kappatools.kcommon.config.ConfigurationException;

public abstract class AbstractAdminHandler extends AbstractHandler {
	private IpMatcherImpl ipMatcher;
	private JSON json = JSON.std.with(JSON.Feature.PRETTY_PRINT_OUTPUT).with(JSON.Feature.FAIL_ON_UNKNOWN_BEAN_PROPERTY).with(JSON.Feature.FAIL_ON_UNKNOWN_TYPE_WRITE).with(JSON.Feature.FAIL_ON_DUPLICATE_MAP_KEYS);

	public AbstractAdminHandler(String adminAllowedNetwork) throws ConfigurationException {
		this.ipMatcher = new IpMatcherImpl(adminAllowedNetwork);
	}

	static public class Result {
		private int HttpCode;
		private Object data;

		public Result(int httpCode, Object data) {
			HttpCode = httpCode;
			this.data = data;
		}

		public int getHttpCode() {
			return HttpCode;
		}

		public Object getData() {
			return data;
		}
	}

	public abstract Result handleRequest(HttpServletRequest request, HttpServletResponse response) throws HttpServerException;

	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		try {
			if (!this.ipMatcher.match(request.getRemoteAddr())) {
				throw new HttpServerException(HttpServletResponse.SC_FORBIDDEN, String.format("Request from %s are not allowed", request.getRemoteAddr()));
			}
			Result r = this.handleRequest(request, response);
			if (r != null) {
				response.setStatus(r.getHttpCode());
				if (r.getData() != null) {
					try {
						String jsonResponse;
						synchronized (r.getData()) {
							jsonResponse = json.asString(r.getData());
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
				}
			} else {
				throw new HttpServerException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Null result object");
			}
		} catch (HttpServerException hse) {
			response.sendError(hse.getErrorCode(), hse.getMessage());
		}
		baseRequest.setHandled(true);
	}

	protected JSON getJson() {
		return this.json;
	}
}
