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

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;

import com.kappaware.kgen.config.Configuration;
import com.kappaware.kgen.config.ConfigurationException;

public class AdminServer {
	private Server server;

	AdminServer(Configuration config) throws ConfigurationException {
		InetSocketAddress bindAddr;
		int port;
		try {
			String[] endp = config.getAdminEndpoint().split(":");
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
			throw new ConfigurationException(String.format("Missing or invalid admin endpoint:%s", config.getAdminEndpoint()));
		}
		this.server = new Server(bindAddr);

		IpMatcherImpl ipMatcher = new IpMatcherImpl();
		for (String segmentDef : config.getAdminAllowedNetwork()) {
			ipMatcher.addSegment(segmentDef);
		}
	}

	void start() throws Exception {
		this.server.start();
	}

	void halt() throws Exception {
		this.server.stop();
	}

	void join() throws InterruptedException {
		this.server.join();
	}

}
