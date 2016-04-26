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
package com.kappaware.kcons;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.console.ConsoleReader;

public class Keyboard extends Thread {
	static Logger log = LoggerFactory.getLogger(Keyboard.class);

	private boolean running = true;
	private Engine engine;

	Keyboard(Engine engine) {
		this.engine = engine;
	}

	enum Action {
		stats, messon, messoff, help, halt, __error__
	}

	@Override
	public void run() {
		log.debug("Keyboard thread running");
		ConsoleReader reader;
		try {
			reader = new ConsoleReader();
		} catch (IOException e1) {
			log.error("Error on console init. Will not handle keyboard", e1);
			return;
		}
		reader.setPrompt("kcons>");
		PrintWriter out = new PrintWriter(reader.getOutput());
		//BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (running) {
			String line;
			try {
				line = reader.readLine();
			} catch (IOException e) {
				log.error("Error in keyboard read()", e);
				line = null;
			}
			if (line == null) {
				running = false;
			} else {
				line = line.trim();
				if (line.length() > 0) {
					Action action;
					try {
						action = Action.valueOf(line);
					} catch (IllegalArgumentException e) {
						action = Action.__error__;
					}
					switch (action) {
						case __error__:
							out.printf("ERROR: Unknow command '%s'\n", line);
						break;
						case help:
							out.printf("Valid command: %s\n", Arrays.asList(Action.class.getEnumConstants()));
						break;
						case messoff:
							out.printf("Massage display is switched off\n");
							engine.setDumpMessage(false);
						break;
						case messon:
							out.printf("Massage display is switched on\n");
							engine.setDumpMessage(true);
						break;
						case stats:
						break;
						case halt:
							out.printf("Exiting!\n");
							engine.halt();
							this.halt();
						break;
					}
				}
			}
		}
	}

	void halt() {
		this.running = false;
		this.interrupt();
	}
}
