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
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static md.PropertiesHelper.fromFile;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
final class ProducingConsumingTest
{
    private static final String PROPERTIES = "src/main/resources/confluent-platform-6.2.0.properties";
    private static final String TOPIC = "some-topic";

    private static final Random RANDOM = new Random();

    @Test
    void produce() throws ExecutionException, InterruptedException, IOException
    {
        final Properties ps = fromFile(PROPERTIES);
        final Producer<String, String> p = new KafkaProducer<>(ps);
        final ProducerRecord<String, String> r = new ProducerRecord<>(
                TOPIC,
                "key-" + RANDOM.nextInt(10),
                "value-" + RANDOM.nextInt(1_000_000)
        );
        final Future<RecordMetadata> f = p.send(r);
        p.flush();
        final RecordMetadata md = f.get();
        log.info("md: {}", md);
        assertThat(md.hasOffset()).isTrue();
        assertThat(md.hasTimestamp()).isTrue();
        p.close();
    }

    @Test
    void consume() throws IOException
    {
        final Properties ps = fromFile(PROPERTIES);
        final Consumer<String, String> c = new KafkaConsumer<>(ps);
        c.subscribe(singletonList(TOPIC));
        log.info("Polling ...");
        final ConsumerRecords<String, String> rs = c.poll(ofSeconds(3));
        rs.forEach(r -> {
            log.info("{} = {} ({})",
                    r.key(),
                    r.value(),
                    r.headers()
            );
        });
        log.info("... polled.");
        c.close();
    }
}