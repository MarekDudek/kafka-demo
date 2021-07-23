package md;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static md.ClusterConfigs.BOOTSTRAP_SERVERS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
final class ProducingTest
{
    private static final String TOPIC = "balanced-availability-durability-one-partition";
    private static final String SERIALIZER = StringSerializer.class.getName();
    private static final String DESERIALIZER = StringDeserializer.class.getName();

    @Test
    void test() throws ExecutionException, InterruptedException
    {
        final NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 3);
        newTopic.configs(ImmutableMap.of("min.insync.replicas", "2"));

        final AdminClient admin = createAdmin();
        admin.createTopics(singletonList(newTopic)).all().get();

        final int start = 0;
        final int end = 1_000;
        produce(start, end);
        consume(start, end);

        admin.deleteTopics(singletonList(TOPIC)).all().get();
        admin.close();
    }

    private static AdminClient createAdmin()
    {
        final Map<String, Object> adminConfig = ImmutableMap.<String, Object>builder().
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterConfigs.BOOTSTRAP_SERVERS).
                build();

        return AdminClient.create(adminConfig);
    }

    private static Producer<String, String> createProducer()
    {
        final Map<String, Object> producerConfig = ImmutableMap.<String, Object>builder().
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER).
                put(VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER).
                put(ACKS_CONFIG, "all").
                build();
        return new KafkaProducer<>(producerConfig);
    }

    private static Consumer<String, String> createConsumer()
    {
        final Map<String, Object> consumerConfig = ImmutableMap.<String, Object>builder().
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_DESERIALIZER_CLASS_CONFIG, DESERIALIZER).
                put(VALUE_DESERIALIZER_CLASS_CONFIG, DESERIALIZER).
                put(GROUP_ID_CONFIG, "test-group").
                put(AUTO_OFFSET_RESET_CONFIG, "earliest").
                build();
        return new KafkaConsumer<>(consumerConfig);
    }

    private static void produce(final int start, final int end)
    {
        final Producer<String, String> producer = createProducer();
        for (int i = start; i < end; i++)
        {
            final int index = i;
            final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key", Integer.toString(index));
            producer.send(record, (metadata, exception) -> {
                        if (nonNull(exception))
                            log.error("Error while sending record #{}", index);
                    }
            );
        }
        producer.flush();
        producer.close();
    }

    private static void consume(final int start, final int end)
    {
        final Consumer<String, String> consumer = createConsumer();
        consumer.subscribe(singletonList(TOPIC));
        int next = start;
        while (next < end)
        {
            final ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));
            for (final ConsumerRecord<String, String> record : records)
            {
                final int current = Integer.parseInt(record.value());
                if (current == next)
                    next++;
                else
                    log.error("Missing at, current = {}, next = {}", current, next);

            }
        }
        consumer.close();
    }
}
