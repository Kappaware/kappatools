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

import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.servlet.http.HttpServletRequest;

import com.kappaware.kappatools.kcommon.ExtTs;
import com.kappaware.kappatools.kcommon.Header;
import com.kappaware.kappatools.kcommon.HeaderBuilder;

public class Key extends Header {

	private String verb;
	private String characterEncoding;
	private Long contentLength;
	private String contentType;
	private Map<String, List<String>> parameters;
	private Map<String, List<String>> headers;
	private String protocol;
	private String remoteAddr;
	private String scheme;
	private String serverName;
	private Integer serverPort;
	private String pathInfo;
	private Boolean truncated;

	private List<String> errors;

	// Needed for JSON deserializer
	public Key() {
	}

	public Key(HttpServletRequest request, ExtTs extTs, HeaderBuilder headerBuilder, int level) {
		super();
		headerBuilder.build(this, extTs, request.getRemoteAddr());

		if (level >= 2) {
			this.remoteAddr = request.getRemoteAddr();
			this.verb = request.getMethod();
			this.characterEncoding = request.getCharacterEncoding();
			this.contentType = request.getContentType();
			this.pathInfo = request.getPathInfo();
			this.contentLength = (request.getContentLengthLong()) == -1 ? null : request.getContentLengthLong();
		}

		if (level >= 3) {
			this.parameters = new HashMap<String, List<String>>();
			for (Enumeration<String> e = request.getParameterNames(); e.hasMoreElements();) {
				String pname = e.nextElement();
				parameters.put(pname, Arrays.asList((String[]) request.getParameterValues(pname)));
			}
			this.protocol = request.getProtocol();
			this.scheme = request.getScheme();
			this.serverName = request.getServerName();
			this.serverPort = request.getServerPort();
			this.headers = new HashMap<String, List<String>>();
			for (Enumeration<String> e = request.getHeaderNames(); e.hasMoreElements();) {
				String hname = e.nextElement();
				List<String> hl = new Vector<String>();
				for (Enumeration<String> e2 = request.getHeaders(hname); e2.hasMoreElements();) {
					hl.add(e2.nextElement());
				}
				headers.put(hname, hl);
			}
		}
	}
	
	public void addError(String error) {
		if(this.errors == null) {
			errors = new Vector<String>();
		}
		this.errors.add(error);
	}

	// getter/setter needed for serializer/deserializer

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getVerb() {
		return verb;
	}

	public void setVerb(String verb) {
		this.verb = verb;
	}

	public String getCharacterEncoding() {
		return characterEncoding;
	}

	public void setCharacterEncoding(String characterEncoding) {
		this.characterEncoding = characterEncoding;
	}

	public Long getContentLength() {
		return contentLength;
	}

	public void setContentLength(Long contentLength) {
		this.contentLength = contentLength;
	}

	public Map<String, List<String>> getParameters() {
		return parameters;
	}

	// Parameter can't be of type Map<String, List<String>> or jackson.jr will fail on beanFrom(...)
	@SuppressWarnings("unchecked")
	public void setParameters(Map<String,?> parameters) {
		this.parameters = (Map<String, List<String>>)parameters;
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public String getRemoteAddr() {
		return remoteAddr;
	}

	public void setRemoteAddr(String remoteAddr) {
		this.remoteAddr = remoteAddr;
	}

	public String getScheme() {
		return scheme;
	}

	public void setScheme(String scheme) {
		this.scheme = scheme;
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public Integer getServerPort() {
		return serverPort;
	}

	public void setServerPort(Integer serverPort) {
		this.serverPort = serverPort;
	}

	public String getPathInfo() {
		return pathInfo;
	}

	public void setPathInfo(String pathInfo) {
		this.pathInfo = pathInfo;
	}

	public Map<String, List<String>> getHeaders() {
		return headers;
	}
	
	// Parameter can't be of type Map<String, List<String>> or jackson.jr will fail on beanFrom(...)
	@SuppressWarnings("unchecked")
	public void setHeaders(Map<String, ?> headers) {
		this.headers = ( Map<String, List<String>>)headers;
	}

	public List<String> getErrors() {
		return errors;
	}

	public void setErrors(List<String> errors) {
		this.errors = errors;
	}

	public Boolean getTruncated() {
		return truncated;
	}

	public void setTruncated(Boolean truncated) {
		this.truncated = truncated;
	}

}
