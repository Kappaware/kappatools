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

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.config.ConfigurationException;


public class AdminServer {
	static Logger log = LoggerFactory.getLogger(AdminServer.class);

	private Server server;

	public AdminServer(String adminEndpoint) throws ConfigurationException {
		InetSocketAddress bindAddr;
		int port;
		try {
			String[] endp = adminEndpoint.split(":");
			if (endp.length == 2) {
				port = Integer.parseInt(endp[1]);
				bindAddr = new InetSocketAddress(endp[0], port);
			} else if (endp.length == 1) {
				port = Integer.parseInt(endp[0]);
				bindAddr = new InetSocketAddress("0.0.0.0", port);
			} else {
				throw new Exception("");
			}
		} catch (Throwable t) {
			throw new ConfigurationException(String.format("Missing or invalid admin endpoint:%s", adminEndpoint));
		}
		this.server = new Server(bindAddr);
	}

	public void start() throws Exception {
		ServerConnector sc = (ServerConnector) this.server.getConnectors()[0];
		log.info(String.format("Starting admin server at %s:%d", sc.getHost(), sc.getPort()));
		this.server.start();
	}

	public void halt() throws Exception {
		this.server.stop();
	}

	public void join() throws InterruptedException {
		this.server.join();
	}

	public void setHandler(Handler handler) {
		this.server.setHandler(handler);
	}

}
