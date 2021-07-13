package md;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static md.PropertiesHelper.loadFromFile;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class ProducerTest
{
    @Test
    void test() throws ExecutionException, InterruptedException, IOException
    {
        final Properties properties = loadFromFile("src/main/resources/first-producer.properties");
        final String topic = properties.getProperty("output.topic.name");
        assertThat(topic).isNotEmpty();
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, "some-key", "some-value");
        final Producer<String, String> producer = new KafkaProducer<>(properties);
        final Future<RecordMetadata> future = producer.send(record);
        final RecordMetadata metadata = future.get();
        log.info("metadata: {}", metadata);
        assertThat(metadata.hasOffset()).isTrue();
        assertThat(metadata.hasTimestamp()).isTrue();
    }
}