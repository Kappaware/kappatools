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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

public class Stats {
	long startTime;
	Map<Integer, PartitionStats> partitionById = new HashMap<Integer, PartitionStats>();
	Map<String, PartitionKeyStats> partitionKeysStatsByKey = new HashMap<String, PartitionKeyStats>();
	
	Stats(Collection<TopicPartition> partitions) {
		this.startTime = System.currentTimeMillis();
		for(TopicPartition tp : partitions) {
			this.partitionById.put(tp.partition(), new PartitionStats());
		}
	}
	
	
	static class PartitionStats {
		int partition;
		long messageCount;
		long lastOffset;
	}
	
	static class PartitionKeyStats {
		int partition;
		int messageCount;
		int lastCounterValue;
		int lastKeyCounterValue;
		int partitionSwitchCount;
		int counterResetCount;
	}
}
