package com.kappaware.k2kj;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.k2kj.config.Configuration;

public class Engine  {
	Logger log = LoggerFactory.getLogger(Engine.class);
	
	private Configuration config;
	KafkaConsumer<Byte[], Byte[]> consumer;
	
	public Engine(Configuration config) {
		this.config = config;
		consumer = new KafkaConsumer<Byte[], Byte[]>(config.getConsumerProperties());
		
	}
	
	
	public void run() {
		consumer.subscribe(Arrays.asList(new String[] { config.getSourceTopic() }), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				if(log.isDebugEnabled()) {
					log.debug(String.format("ConsumerRebalanceListener - Revoked partitions: %s", partitions.stream().map(TopicPartition::partition).collect(Collectors.toList())  ));
				}
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				log.debug(String.format("ConsumerRebalanceListener - Assigned partitions: %s", partitions.stream().map(TopicPartition::partition).collect(Collectors.toList())  ));
			}
		});
		
		
		
	}

}
