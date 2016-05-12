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

import java.util.List;

import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kappatools.kcommon.Engine;
import com.kappaware.kappatools.kcommon.config.Settings;
import com.kappaware.kappatools.kcommon.stats.AbstractStats;
import com.kappaware.king.config.Configuration;

public class EngineImpl extends Thread implements Engine {
	static Logger log = LoggerFactory.getLogger(EngineImpl.class);

	private boolean running = true;
	private Stats stats;
	private Settings settings;
	
	public EngineImpl(Configuration config) {
		this.settings = config.getSettings();
		
	}

	public void init(List<PartitionInfo> partitionsFor) {
		this.stats = new Stats(partitionsFor);
	}

	@Override
	public AbstractStats getStats() {
		return this.stats;
	}

	@Override
	public Settings getSettings() {
		return this.settings;
	}

	

	@Override
	public void run() {
		while (running) {
			try {
				Thread.sleep(this.settings.getSamplingPeriod());
			} catch (InterruptedException e) {
				log.debug("Interrupted in normal sleep!");
			}
			this.stats.tick();
			if(this.settings.getStatson()) {
				log.info(this.stats.getProducerStats().toString());
			}
		}
	}
	
	
	void halt() {
		this.running = false;
		this.interrupt();
	}

	public void addToStats(byte[] key, int partition, long offset) {
		this.stats.addToProducerStats(key, partition, offset);
	}

	public boolean isMesson() {
		return this.settings.getMesson();
	}
}
