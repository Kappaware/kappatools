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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.jr.ob.JSON;

public class Stats {
	private static Logger log = LoggerFactory.getLogger(Stats.class);

	private static JSON json = JSON.std;

	private long startTime;
	private Map<Integer, PartitionStats> partitionById = new HashMap<Integer, PartitionStats>();
	private Map<String, PartitionKeyStats> partitionKeysByKey = new HashMap<String, PartitionKeyStats>();
	private List<String> errors = new Vector<String>();
	private RateHolder rateHolder = new RateHolder();

	Stats(Collection<TopicPartition> partitions) {
		this.startTime = System.currentTimeMillis();
		for (TopicPartition tp : partitions) {
			this.partitionById.put(tp.partition(), new PartitionStats(tp.partition()));
		}
	}

	private void error(String mess) {
		this.errors.add(mess);
		log.warn(mess);
	}

	void add(ConsumerRecord<Object, Object> record) {
		this.rateHolder.inc();
		PartitionStats pt = partitionById.get(record.partition());
		if (pt == null) {
			log.error(String.format("Getting a message on a non-assigned partition #%d", record.partition()));
		} else {
			pt.inc();
			if (record.offset() != pt.lastOffset + 1 && pt.lastOffset != 0) {
				error(String.format("Offset mistmatch on partition %d: lastOffset:%d  record.offset:%d", record.partition(), pt.lastOffset, record.offset()));
			}
			pt.lastOffset = record.offset();
		}
		Object key = record.key();
		Header h = null;
		if (key instanceof Header) {
			h = (Header) key;
		} else {
			try {
				h = json.beanFrom(Header.class, key.toString());
			} catch (IOException e) {
				error(String.format("Unable to parse '%s' as Header", key.toString()));
			}
		}
		if (h != null) {
			PartitionKeyStats pks = this.partitionKeysByKey.get(h.getPartitionKey());
			if (pks == null) {
				pks = new PartitionKeyStats(h.getPartitionKey());
				this.partitionKeysByKey.put(h.getPartitionKey(), pks);
			}
			pks.messageCount++;
			if (pks.partition != record.partition()) {
				if (pks.partition != -1) {
					error(String.format("Key '%s' is switching from partition#%d to partition#%d!", h.getPartitionKey(), pks.partition, record.partition()));
				}
				pks.partition = record.partition();
			}
			if (pks.lastKeyCounter + 1 != h.getKeyCounter() && h.getKeyCounter() != 0 && pks.lastKeyCounter != -1) {
				error(String.format("Key counter missmatch on key '%s'. Last was %d while received %d", h.getPartitionKey(), pks.lastKeyCounter, h.getKeyCounter()));
			}
			pks.lastKeyCounter = h.getKeyCounter();
			if (pks.lastKeyCounter == 0) {
				pks.keyCounterResetCount++;
			}
		}
	}
	
	static class RateHolder {
		private long startTime;
		private long count;

		private long lastReadTime;
		private long lastReadCount;
		private Double lastRate;

		RateHolder() {
			this.startTime = System.currentTimeMillis();
			this.lastReadTime = this.startTime;
		}
		
		public Double getGlobalRate() {
			long now = System.currentTimeMillis();
			return (new Double(count) * 1000) / (now - this.startTime);
		}
		
		void tick() {
			long now;
			long lastCount;
			synchronized(this) {
				now = System.currentTimeMillis();
				lastCount = this.count;
			}
			this.lastRate = (new Double(lastCount - this.lastReadCount) * 1000) / (now - this.lastReadTime);
			this.lastReadCount = lastCount;
			this.lastReadTime = now;
		}
		
		synchronized  void inc() {
			this.count++;
		}
		
		Double getLastRate() {
			return this.lastRate;
		}
		
		public Double getRate() {
			this.tick();
			return this.lastRate;
		}
		
		public long getCount() {
			return this.count;
		}
	}

	static class PartitionStats {
		private int partition;
		private long lastOffset;
		private RateHolder rateHolder = new RateHolder();
		

		public PartitionStats(int partition) {
			this.partition = partition;
		}

		public long getMessageCount() {
			return this.rateHolder.getCount();
		}

		public long getLastOffset() {
			return lastOffset;
		}
		
		public void inc() {
			this.rateHolder.inc();
		}
		
		@Override
		public String toString() {
			return String.format("Partition:%d  messageCount:%d  lastOffset:%d   Read rate: %.2f mess/sec", this.partition, this.rateHolder.getCount(), this.lastOffset, this.rateHolder.getRate());
		}

	}

	static class PartitionKeyStats {
		String partitionKey;
		long messageCount;
		int partition = -1;
		long lastKeyCounter = -1;
		int keyCounterResetCount;

		public PartitionKeyStats(String partitionKey) {
			this.partitionKey = partitionKey;
		}

		public long getMessageCount() {
			return messageCount;
		}

		public int getPartition() {
			return partition;
		}

		public long getLastKeyCounter() {
			return lastKeyCounter;
		}

		public int getKeyCounterResetCount() {
			return keyCounterResetCount;
		}

		@Override
		public String toString() {
			return String.format("Key:%10s  partition:%d  lastKeyCounter:%d  keyCounterResetCount:%d", this.partitionKey, this.partition, this.lastKeyCounter, this.keyCounterResetCount);
		}

	}

	public String getStartTime() {
		return Utils.printIsoDateTime(startTime);
	}

	public Map<Integer, PartitionStats> getPartitionById() {
		return partitionById;
	}

	public Map<String, PartitionKeyStats> getPartitionKeysByKey() {
		return partitionKeysByKey;
	}

	public List<String> getErrors() {
		return errors;
	}

	@Override
	public String toString() {
		return this.toString(1);
	}

	public String toString(int level) {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("Start time:%s    Read rate: %.2f mess/sec\n", this.getStartTime(), this.rateHolder.getRate()));
		List<Integer> l = new Vector<Integer>(this.partitionById.keySet());
		Collections.sort(l);
		for (Integer p : l) {
			sb.append(this.partitionById.get(p).toString());
			sb.append("\n");
		}
		if (level > 1) {
			List<String> ls = new Vector<String>(this.partitionKeysByKey.keySet());
			Collections.sort(ls);
			for (String pk : ls) {
				sb.append(this.partitionKeysByKey.get(pk).toString());
				sb.append("\n");
			}
		}
		return sb.toString();
	}

}
