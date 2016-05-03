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
package com.kappaware.kappatools.kcommon;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.kafka.common.PartitionInfo;
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
	private ThroughputMeter throughputMeter = new ThroughputMeter();
	private Long lastCounter; 
	private long maxCounter = 0;
	
	public Stats(Collection<TopicPartition> partitions) {
		this.startTime = System.currentTimeMillis();
		for (TopicPartition tp : partitions) {
			this.partitionById.put(tp.partition(), new PartitionStats(tp.partition()));
		}
	}

	public Stats(List<PartitionInfo> partitions) {
		this.startTime = System.currentTimeMillis();
		for (PartitionInfo tp : partitions) {
			this.partitionById.put(tp.partition(), new PartitionStats(tp.partition()));
		}
	}

	private void error(String mess) {
		this.errors.add(mess);
		log.warn(mess);
	}
	
	public void tick() {
		this.throughputMeter.tick();
		for(PartitionStats ps : this.partitionById.values()) {
			ps.throughputMeter.tick();
		}
	}

	public synchronized void add(Object key, int partition, long offset) {
		this.throughputMeter.inc();
		PartitionStats pt = partitionById.get(partition);
		if (pt == null) {
			log.error(String.format("Getting a message on a non-assigned partition #%d", partition));
		} else {
			pt.inc();
			if (offset != pt.lastOffset + 1 && pt.lastOffset != 0) {
				error(String.format("Offset mistmatch on partition %d: lastOffset:%d  record.offset:%d",partition, pt.lastOffset, offset));
			}
			pt.lastOffset = offset;
		}
		Header header = null;
		if (key instanceof Header) {
			header = (Header) key;
		} else {
			try {
				header = json.beanFrom(Header.class, key.toString());
			} catch (IOException e) {
				error(String.format("Unable to parse '%s' as Header", key.toString()));
			}
		}
		if (header != null) {
			this.lastCounter = header.getExtTs().getCounter();
			if(this.maxCounter < header.getExtTs().getCounter()) {
				this.maxCounter = header.getExtTs().getCounter();
			}
			PartitionKeyStats pks = this.partitionKeysByKey.get(header.getPartitionKey());
			if (pks == null) {
				pks = new PartitionKeyStats(header.getPartitionKey());
				this.partitionKeysByKey.put(header.getPartitionKey(), pks);
			}
			pks.messageCount++;
			if (pks.partition != partition) {
				if (pks.partition != -1) {
					error(String.format("Key '%s' is switching from partition#%d to partition#%d!", header.getPartitionKey(), pks.partition,partition));
				}
				pks.partition = partition;
			}
			if (pks.lastKeyCounter + 1 != header.getKeyCounter() && header.getKeyCounter() != 0 && pks.lastKeyCounter != -1) {
				error(String.format("Key counter missmatch on key '%s'. Last was %d while received %d", header.getPartitionKey(), pks.lastKeyCounter, header.getKeyCounter()));
			}
			pks.lastKeyCounter = header.getKeyCounter();
			if (pks.lastKeyCounter == 0) {
				pks.keyCounterResetCount++;
			}
		}
	}
	
	static class ThroughputMeter {
		private long startTime;
		private long count;

		private long lastReadTime;
		private long lastReadCount;
		private Double lastThroughput;

		ThroughputMeter() {
			this.startTime = System.currentTimeMillis();
			this.lastReadTime = this.startTime;
		}
		
		public Double getOverallThroughput() {
			long now = System.currentTimeMillis();
			//return (new Double(count) * 1000) / (now - this.startTime);
			return new Double((count * 100000) / (now - this.startTime)) / 100;
		}
		
		void tick() {
			long now;
			long lastCount;
			synchronized(this) {
				now = System.currentTimeMillis();
				lastCount = this.count;
			}
			this.lastThroughput = new Double( ((lastCount - this.lastReadCount) * 100000) / (now - this.lastReadTime)) / 100;
			this.lastReadCount = lastCount;
			this.lastReadTime = now;
		}
		
		synchronized  void inc() {
			this.count++;
		}
		
		public Double getLastThroughput() {
			return this.lastThroughput;
		}
		
		public long getCount() {
			return this.count;
		}
	}

	static class PartitionStats {
		private int partition;
		private long lastOffset;
		private ThroughputMeter throughputMeter = new ThroughputMeter();
		
		public void inc() {
			this.throughputMeter.inc();
		}
		
		@Override
		public String toString() {
			return String.format("Partition:%d  messageCount:%d  lastOffset:%d   Last Throughput: %.2f mess/sec", this.partition, this.throughputMeter.getCount(), this.lastOffset, this.throughputMeter.getLastThroughput());
		}

		// Public Getter. Will be viewed by JSON stats
		public PartitionStats(int partition) {
			this.partition = partition;
		}

		public long getLastOffset() {
			return lastOffset;
		}

		public Double getOverallThroughput() {
			return throughputMeter.getOverallThroughput();
		}

		public Double getLastThroughput() {
			return throughputMeter.getLastThroughput();
		}

		public long getCount() {
			return throughputMeter.getCount();
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

		public long getCount() {
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
	@Override
	public String toString() {
		return this.toString(1);
	}

	public String toString(int level) {
		StringBuffer sb0 = new StringBuffer();
		List<Integer> l = new Vector<Integer>(this.partitionById.keySet());
		Collections.sort(l);
		String sep = "";
		for (Integer p : l) {
			sb0.append(String.format("%s%d:%d/%d", sep, p, this.partitionById.get(p).throughputMeter.getCount(), this.partitionById.get(p).lastOffset));
			sep = ", ";
		}
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("Counter:%d  Throughput: %.2f mess/sec  Partition:Count/offset:%s", this.maxCounter, this.throughputMeter.getLastThroughput(), sb0.toString()));
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

	// Public getters. Will be viewed by JSON stats
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

	public Double getOverallThroughput() {
		return throughputMeter.getOverallThroughput();
	}

	public Double getLastThroughput() {
		return throughputMeter.getLastThroughput();
	}

	public long getCount() {
		return throughputMeter.getCount();
	}

	public Long getLastCounter() {
		return lastCounter;
	}

	public long getMaxCounter() {
		return maxCounter;
	}
	
	
}
