package com.kappaware.kgen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kgen.config.Configuration;
import com.kappaware.kgen.config.ConfigurationException;
import com.kappaware.kgen.config.ConfigurationImpl;
import com.kappaware.kgen.config.Parameters;


public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws Exception {

		log.trace("Entering Main");

		Configuration config;
		try {
			config = new ConfigurationImpl(new Parameters(argv));
			Engine engine = new Engine(config);
			engine.run();
		} catch(ConfigurationException e) {
			log.error(e.getMessage());
			System.exit(1);
		}
		catch (Exception e) {
			log.error("Error on launch!", e);
			System.exit(2);
		}
	}
}
