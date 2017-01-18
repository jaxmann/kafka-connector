import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaDriver {

	public static void main(String[] args) {
		
		String topic = "twitter";

		Properties props = new Properties();
		props.put("metadata.broker.list", "<server_ip>:9092"); //kafka
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "SimplePartitioner");
		props.put("controlled.shutdown.max.retries", 0);
		props.put("request.required.acks", "0");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "all");
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		
		String csvFile = args[0];
		BufferedReader br = null;
		String line = "";
		
		try {
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, line);
				producer.send(data);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

			
		producer.close();

		

	}

}
