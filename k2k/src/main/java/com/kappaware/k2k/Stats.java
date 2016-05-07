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
package com.kappaware.k2k;

import java.util.Collection;
import java.util.List;
import java.util.Stack;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import com.kappaware.kappatools.kcommon.stats.AbstractStats;
import com.kappaware.kappatools.kcommon.stats.ConsumerStats;
import com.kappaware.kappatools.kcommon.stats.ProducerStats;

public class Stats extends AbstractStats {
	
	private List<PartitionInfo> producerPartition;
	private ConsumerStats consumerStats;
	private ProducerStats producerStats;
	private Stack<HistoryItem> history = new Stack<HistoryItem>();
	
	static class HistoryItem {
		ConsumerStats consumerStats;
		ProducerStats producerStats;
		
		public HistoryItem(ConsumerStats consumerStats, ProducerStats producerStats) {
			this.consumerStats = consumerStats;
			this.producerStats = producerStats;
		}
		public ConsumerStats getConsumerStats() {
			return consumerStats;
		}
		public ProducerStats getProducerStats() {
			return producerStats;
		}
	}
	
	Stats(List<PartitionInfo> producerPartition) {
		this.producerPartition = producerPartition;
	}
	
	void newConsumerStats(Collection<TopicPartition> partitions) {
		if(this.consumerStats != null) {
			this.consumerStats.prepareToHistorization();
			this.producerStats.prepareToHistorization();
			
			this.history.push(new HistoryItem(this.consumerStats, this.producerStats));
		}
		this.consumerStats = new ConsumerStats(partitions);
		this.producerStats = new ProducerStats(this.producerPartition);
	}
	
	
	
	public void addToConsumerStats(byte[] key, int partition, long offset) {
		this.consumerStats.add(key, partition, offset);
	}
	
	public void addToProducerStats(byte[] key, int partition, long offset) {
		this.producerStats.add(key, partition, offset);
	}

	@Override
	public void tick() {
		this.consumerStats.tick();
		this.producerStats.tick();
	}

	public ConsumerStats getConsumerStats() {
		return consumerStats;
	}

	public ProducerStats getProducerStats() {
		return producerStats;
	}

	public Stack<HistoryItem> getHistory() {
		return this.history;
	}
	
	

}
