package com.kappaware.k2kj;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.k2kj.config.Configuration;
import com.kappaware.k2kj.config.ConfigurationException;
import com.kappaware.k2kj.config.ConfigurationImpl;
import com.kappaware.k2kj.config.Parameters;


public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws Exception {

		log.trace("Entering Main");

		Configuration config;
		try {
			config = new ConfigurationImpl(new Parameters(argv));
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
