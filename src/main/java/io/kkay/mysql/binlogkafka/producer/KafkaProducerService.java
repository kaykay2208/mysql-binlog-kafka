package io.kkay.mysql.binlogkafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerService {

	private final KafkaProducer<String, String> producer;
	private final String topic;

	public KafkaProducerService(KafkaProducer<String, String> producer, String topic) {
		this.producer = producer;
		this.topic = topic;
	}

	public void sendMessage(String message) {
		producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
			if (exception != null) {
				System.err.println("Failed to send message: " + exception.getMessage());
			}
		});
	}

	public void close() {
		producer.close();
	}
}
