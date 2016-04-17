package com.kappaware.kgen;

import java.util.List;
import java.util.Vector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.kgen.config.Configuration;

public class Engine extends Thread {
	static Logger log = LoggerFactory.getLogger(Engine.class);

	private boolean running = true;
	private Configuration config;
	private KafkaProducer<Key, String> producer;
	private ExtTsFactory factory;
	private long startTime;
	private long lastPrintStats = 0L;
	private Stats stats = new Stats();

	Engine(Configuration config) {
		this.config = config;
		this.producer = new KafkaProducer<Key, String>(config.getProducerProperties(), new JsonSerializer<Key>(false), new StringSerializer());
		this.factory = new ExtTsFactory(config.getGateId(), config.getInitialCounter());
	}

	public void run() {
		startTime = System.currentTimeMillis();
		while (running) {
			int partitionCount = this.producer.partitionsFor(this.config.getTargetTopic()).size();
			//log.trace(String.format("Partition count:%d", partitionCount));

			for (int i = 0; i < config.getBurstCount(); i++) {
				ExtTs extTs = this.factory.get();
				Key key = new Key(extTs);
				String value = String.format("Message #%d for %s from %s", extTs.getCounter(), key.getRecipient(), extTs.getGateId());
				int partition = Utils.abs(Utils.murmur2(key.getRecipient().getBytes())) % partitionCount;
				//log.trace(String.format("Pushing message to kafka to partition %d", partition));
				producer.send(new ProducerRecord<Key, String>(this.config.getTargetTopic(), partition, key, value));
				this.stats.inc(partition);
			}
			try {
				Thread.sleep(config.getPeriod());
			} catch (InterruptedException e) {
				log.debug("Interrupted in normal sleep!");
			}
			this.printStats("", false);
		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// This case is normal in case of period = 0. In such case, interrupted flag was not cleared
			log.debug("Interrupted in end of thread processing!");
		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			log.warn("Too many interruption in end of thread processing!");
		}
		this.producer.flush();
		this.producer.close();
		log.info("KGEN END");
		this.printStats("", true);
		log.info(String.format("Next counter:%d", this.factory.getNextCounter()));
	}

	void halt() {
		this.running = false;
		this.interrupt();
	}

	static class Stats {
		private int count = 0;
		private List<Integer> countByPartition = new Vector<Integer>();

		void inc(int partition) {
			count++;
			for (int p = this.countByPartition.size(); p <= partition; p++) {
				this.countByPartition.add(p, new Integer(0));
			}
			this.countByPartition.set(partition, this.countByPartition.get(partition) + 1);
		}

		public String partitionStats() {
			StringBuffer sb = new StringBuffer();
			String sep = "";
			for (int p = 0; p < this.countByPartition.size(); p++) {
				sb.append(String.format("%sp%d:%d", sep, p, this.countByPartition.get(p)));
				sep = ", ";
			}
			return sb.toString();
		}

		public int getCount() {
			return this.count;
		}

	}

	void printStats(String prefix, boolean force) {
		long now = System.currentTimeMillis();
		if ((this.config.getStatsPeriod() != 0 && this.lastPrintStats + this.config.getStatsPeriod() < now) || force) {
			this.lastPrintStats = now;
			long ellapsed = now - this.startTime;
			long nbrSec = (ellapsed / 1000);
			double messPerSecond = (new Double(this.stats.getCount()) / ellapsed) * 1000;
			long nbrHours = nbrSec / 3600;
			long nbrMinutes = nbrSec / 60 - (nbrHours * 60);
			long nbrSec2 = nbrSec - (nbrMinutes * 60) - (nbrHours * 3600);
			log.info(String.format("%sAfter %02d:%02d:%02d, sent %d messages (%.2f mess/sec).  By partition: %s", prefix, nbrHours, nbrMinutes, nbrSec2, this.stats.getCount(), messPerSecond, this.stats.partitionStats()));
		}

	}

}
