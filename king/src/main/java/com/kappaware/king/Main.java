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

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.config.ConfigurationException;
import com.kappaware.kappatools.kcommon.jetty.AdminHandler;
import com.kappaware.kappatools.kcommon.jetty.AdminServer;
import com.kappaware.king.config.Configuration;
import com.kappaware.king.config.ConfigurationImpl;
import com.kappaware.king.config.ParametersImpl;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws Exception {

		log.info("king start");

		Configuration config;
		try {
			config = new ConfigurationImpl(new ParametersImpl(argv));
			EngineImpl engine = new EngineImpl(config);
			Server mainServer = buildMainServer(config, engine);
			final AdminServer adminServer = config.getAdminBindAddress() != null ? new AdminServer(config.getAdminBindAddress()) : null;
			if(adminServer != null) {
				adminServer.setHandler(new AdminHandler(config.getAdminNetworkFilter(), engine));
			}
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					log.debug("Shutdown hook called!");
					if(adminServer != null) {
						try {
							adminServer.halt();
							adminServer.join();
						} catch (Exception e1) {
							log.error("Error in adming server shutdown", e1);
						}
					}
					engine.halt();
					try {
						engine.join();
					} catch (InterruptedException e) {
						log.debug("Interrupted in join");
					}
					try {
						mainServer.stop();
						mainServer.join();
					} catch (Exception e1) {
						log.error("Error in main server shutdown", e1);
					}
					try {
						sleep(100); // To let message to be drained
					} catch (InterruptedException e) {
						log.debug("Interrupted in sleep");
					}
					log.info("king end");
				}
			});

			mainServer.start();
			engine.start();
			if(adminServer != null) {
				adminServer.start();
			}
		} catch (ConfigurationException e) {
			log.error(e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			log.error("Error on launch!", e);
			System.exit(2);
		}
	}
	

	static public Server buildMainServer(Configuration config, EngineImpl engine) {
		Server server = new Server(config.getMainBindAddress());
		ServletHandler handler = new ServletHandler();
		server.setHandler(handler);
		ServletHolder servletHolder = new ServletHolder(new KingServlet(config, engine));
		servletHolder.setAsyncSupported(true);
		handler.addServletWithMapping(servletHolder, "/*");
		return server;
	}
}
