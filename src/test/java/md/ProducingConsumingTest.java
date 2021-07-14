package md;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.time.Duration.ofSeconds;
import static md.PropertiesHelper.fromFile;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
class ProducingConsumingTest
{
    @Test
    void produce() throws ExecutionException, InterruptedException, IOException
    {
        final Properties properties = fromFile("src/main/resources/confluent-platform-6.2.0.properties");
        final ProducerRecord<String, String> record = new ProducerRecord<>("some-topic", "some-key", "some-value");
        final Producer<String, String> producer = new KafkaProducer<>(properties);
        final Future<RecordMetadata> future = producer.send(record);
        final RecordMetadata metadata = future.get();
        log.info("metadata: {}", metadata);
        assertThat(metadata.hasOffset()).isTrue();
        assertThat(metadata.hasTimestamp()).isTrue();
        producer.close();
    }

    @Test
    void consume() throws IOException
    {
        final Properties properties = fromFile("src/main/resources/confluent-platform-6.2.0.properties");
        final Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("some-topic"));
        log.info("Polling ...");
        final ConsumerRecords<String, String> records = consumer.poll(ofSeconds(10));
        records.forEach(record -> {
            log.info("{}:{} ({})", record.key(), record.value(), record.headers());
        });
        consumer.close();
    }
}