package md;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerApplication
{
    public static void main(String[] args) throws ExecutionException, InterruptedException
    {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put("input.topic.name", "input-topic");
        properties.put("output.topic.name", "output-topic");
        final Producer<String, String> producer = new KafkaProducer<>(properties);
        System.out.println(producer);
        final String topic = properties.getProperty("output.topic.name");
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, "my-key", "my-value");
        final Future<RecordMetadata> future = producer.send(record);
        final RecordMetadata metadata = future.get();
        System.out.println(metadata);
        producer.close();
    }
}
