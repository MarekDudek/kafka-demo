package md;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static java.util.Optional.empty;
import static md.ClusterConfigs.BOOTSTRAP_SERVERS;
import static md.Sleep.sleep;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
@Disabled
final class FailingCommitTest
{
    private static final String TOPIC = "failing-commit-test";

    private static AdminClient createAdmin()
    {
        final Map<String, Object> adminConfig = ImmutableMap.<String, Object>builder().
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                build();
        return AdminClient.create(adminConfig);
    }

    @Test
    void create_topic() throws ExecutionException, InterruptedException
    {
        final AdminClient admin = createAdmin();
        admin.createTopics(singletonList(new NewTopic(TOPIC, empty(), empty()))).all().get();
        admin.close();
    }

    @Test
    void delete_topic() throws ExecutionException, InterruptedException
    {
        final AdminClient admin = createAdmin();
        admin.deleteTopics(singletonList(TOPIC)).all().get();
        admin.close();
    }

    private static KafkaProducer<String, String> createProducer()
    {
        final Map<String, Object> producerConfig = ImmutableMap.<String, Object>builder().
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(ACKS_CONFIG, "all").
                build();
        return new KafkaProducer<>(producerConfig);
    }

    @Test
    void send_messages()
    {
        final Producer<String, String> producer = createProducer();
        for (int i = 0; i < 100; i++)
        {
            final int msgNo = i;
            producer.send(new ProducerRecord<>(TOPIC, "key", "value-" + i), (metadata, exception) -> {
                        if (nonNull(exception))
                        {
                            log.error("Error while sending message {} {}", msgNo, exception);
                        }
                    }
            );
        }
        producer.flush();
        producer.close();
    }

    private static KafkaConsumer<String, String> createConsumer()
    {
        final Map<String, Object> consumerConfig = ImmutableMap.<String, Object>builder().
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()).
                put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()).
                put(GROUP_ID_CONFIG, "test-group").
                put(AUTO_OFFSET_RESET_CONFIG, "earliest").
                put(ENABLE_AUTO_COMMIT_CONFIG, false).
                put(MAX_POLL_RECORDS_CONFIG, 10).
                put(SESSION_TIMEOUT_MS_CONFIG, 6000).

                build();
        return new KafkaConsumer<>(consumerConfig);

    }

    @Test
    void receive_messages()
    {
        final KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(singletonList(TOPIC));

        boolean more = true;
        while (more)
        {
            log.info("Polling");
            final ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));
            if (records.isEmpty())
                more = false;
            records.forEach(record -> {
                log.info("{}", record);
                sleep(ofSeconds(2));
            });
            consumer.commitSync();
        }
        consumer.close();
    }

    @Test
    void polling_twice_without_commit_repeats_messages()
    {

    }
}
