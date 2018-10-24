package com.dcai.kafka.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {

	public static void main(String[] args) {

		Properties properties = new Properties();

		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		properties.setProperty("acks", "1");
		properties.setProperty("retries", "3");
		properties.setProperty("linger.ms", "1");

		Producer<String, String> producer = new KafkaProducer<>(properties);

		for (int key = 1; key < 10; key++) {
			String keyStr = Integer.toString(key);
			ProducerRecord<String, String> producerRecord
			        = new ProducerRecord<>("second_topic", keyStr, "message test " + keyStr);
			producer.send(producerRecord);
			producer.flush();
		}

		producer.close();

	}

}
