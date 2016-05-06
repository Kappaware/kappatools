package com.kappaware.k2k;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.k2k.config.Configuration;
import com.kappaware.k2k.config.ConfigurationException;
import com.kappaware.k2k.config.ConfigurationImpl;
import com.kappaware.k2k.config.ParametersImpl;


public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws Exception {

		log.trace("Entering Main");

		Configuration config;
		try {
			config = new ConfigurationImpl(new ParametersImpl(argv));
			
			
			
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
