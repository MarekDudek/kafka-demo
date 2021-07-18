package md;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static md.PropertiesHelper.fromFile;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class ProducingConsumingTest
{
    private static final String PROPERTIES = "src/main/resources/confluent-platform-6.2.0-multi-broker.properties";
    private static final String PRODUCER_PROPERTIES = "src/main/resources/confluent-platform-6.2.0-multi-broker-producer.properties";
    private static final String CONSUMER_PROPERTIES = "src/main/resources/confluent-platform-6.2.0-multi-broker-consumer.properties";
    private static final String TOPIC = "some-topic";

    private static final Random RANDOM = new Random();

    @Test
    @Order(1)
    void createTopic() throws ExecutionException, InterruptedException
    {
        final Properties ps = fromFile(PROPERTIES);
        AdminClient a = AdminClient.create(ps);
        final NewTopic t = new NewTopic(TOPIC, empty(), empty());
        final CreateTopicsResult ts = a.createTopics(singletonList(t));
        final KafkaFuture<Void> f = ts.all();
        f.get();
        a.close();
    }

    @Test
    @Order(2)
    void produce() throws ExecutionException, InterruptedException
    {
        final Properties ps = fromFile(PROPERTIES, PRODUCER_PROPERTIES);
        final Producer<String, String> p = new KafkaProducer<>(ps);
        final ProducerRecord<String, String> r = new ProducerRecord<>(
                TOPIC,
                "key-" + RANDOM.nextInt(10),
                "value-" + RANDOM.nextInt(1_000_000)
        );
        final Future<RecordMetadata> f = p.send(r);
        p.flush();
        final RecordMetadata md = f.get();
        log.info("Got {}", md);
        assertThat(md.hasOffset()).isTrue();
        assertThat(md.hasTimestamp()).isTrue();
        p.close();
    }

    @Test
    @Order(3)
    void consume()
    {
        final Properties ps = fromFile(PROPERTIES, CONSUMER_PROPERTIES);
        final Consumer<String, String> c = new KafkaConsumer<>(ps);
        c.subscribe(singletonList(TOPIC));
        log.info("Polling ...");
        final ConsumerRecords<String, String> rs = c.poll(ofSeconds(30));
        assertThat(rs.isEmpty()).isFalse();
        rs.forEach(r -> {
            log.info("Got {} = {} ({})",
                    r.key(),
                    r.value(),
                    r.headers()
            );
        });
        log.info("... polled.");
        c.close();
    }

    @Test
    @Order(4)
    void deleteTopic() throws ExecutionException, InterruptedException
    {
        final Properties ps = fromFile(PROPERTIES);
        AdminClient a = AdminClient.create(ps);
        final DeleteTopicsResult ts = a.deleteTopics(singletonList(TOPIC));
        final KafkaFuture<Void> f = ts.all();
        f.get();
        a.close();
    }
}