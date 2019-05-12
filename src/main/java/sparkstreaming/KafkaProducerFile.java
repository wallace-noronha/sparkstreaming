package sparkstreaming;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class KafkaProducerFile {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		final String fileName = "C:\\Users\\wal_f\\eclipse-workspace\\sparkstreaming\\src\\main\\resources\\oscar_age_male.json";
		String line;
		String topicName = "test";
		final KafkaProducer<String, String> kafkaProducer;
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("client.id","KafkaFileProducer");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProducer  = new KafkaProducer<String, String>(properties);
		for(int i = 0; i < 50; i++) {
			int count = 0;
			try(BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))){
				while ((line = bufferedReader.readLine()) != null) {
					count++;
					kafkaProducer.send(new ProducerRecord<String, String>(topicName, Integer.toString(count), line));
				}
			} catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
