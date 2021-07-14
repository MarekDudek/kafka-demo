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

import static md.PropertiesHelper.fromFile;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class ProducerTest
{
    @Test
    void producing() throws ExecutionException, InterruptedException, IOException
    {
        final Properties properties = fromFile("src/main/resources/confluent-platform-6.2.0.properties");
        final ProducerRecord<String, String> record = new ProducerRecord<>("some-topic", "some-key", "some-value");
        final Producer<String, String> producer = new KafkaProducer<>(properties);
        final Future<RecordMetadata> future = producer.send(record);
        final RecordMetadata metadata = future.get();
        log.info("metadata: {}", metadata);
        assertThat(metadata.hasOffset()).isTrue();
        assertThat(metadata.hasTimestamp()).isTrue();
    }
}