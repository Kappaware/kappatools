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
package com.kappaware.kgen;

import java.util.List;

import org.apache.kafka.common.PartitionInfo;

import com.kappaware.kappatools.kcommon.stats.AbstractStats;
import com.kappaware.kappatools.kcommon.stats.ProducerStats;

public class Stats extends AbstractStats {
	private ProducerStats producerStats;
	
	Stats(List<PartitionInfo> producerPartition) {
		this.producerStats = new ProducerStats(producerPartition);
	}
	
	
	public void addToProducerStats(byte[] key, int partition, long offset) {
		this.producerStats.add(key, partition, offset);
	}

	@Override
	public void tick() {
		this.producerStats.tick();
	}

	public ProducerStats getProducerStats() {
		return producerStats;
	}


}
