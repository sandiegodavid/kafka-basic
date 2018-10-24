package com.dcai.kafka.basic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemo {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumerDemo.class);

	public static void main(String[] args) {

		Properties properties = new Properties();

		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		properties.setProperty("group.id", "test");
		properties.setProperty("enable.auto.commit", "false");
		properties.setProperty("auto.commit.interval.ms", "1000");

		properties.setProperty("auto.offset.reset", "earliest");

		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {

			kafkaConsumer.subscribe(Arrays.asList("first_topic", "second_topic"));
			while (true) {
				ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : consumerRecord) {
					System.out.println("record: value:" + record.value()
					        + ", key:" + record.key()
					        + ", topic:" + record.topic()
					        + ", partition:" + record.partition()
					        + ", offset:" + record.offset()
					        + ", timestamp:" + record.timestamp());
				}
				kafkaConsumer.commitAsync();
			}
		}
	}

}
