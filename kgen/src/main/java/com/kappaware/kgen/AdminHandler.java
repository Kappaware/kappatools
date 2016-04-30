package com.kappaware.kgen;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.kappaware.kgen.jetty.AbstractAdminHandler;
import com.kappaware.kgen.jetty.HttpServerException;
import com.kappaware.kgen.jetty.IpMatcher;

public class AdminHandler extends AbstractAdminHandler {
	private Engine engine;

	public AdminHandler(IpMatcher ipMatcher, Engine engine) {
		
		
	
		super(ipMatcher);
		this.engine = engine;
	}

	@Override
	public Object handleRequest(HttpServletRequest request, HttpServletResponse response) throws HttpServerException {
		if(request.getMethod().equals("GET")) {
			String pathInfo = request.getPathInfo();
			if(pathInfo.equalsIgnoreCase("stats")) {
				return engine.getStats();
			} else {
				throw new HttpServerException(HttpServletResponse.SC_NOT_FOUND, String.format("Invalid path info:'%s'!", pathInfo));
			}
		} else {
			throw new HttpServerException(HttpServletResponse.SC_NOT_FOUND, "Only GET method handled here!");
		}
	}

}
