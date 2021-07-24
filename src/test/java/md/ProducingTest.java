package md;

import com.google.common.collect.ImmutableMap;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static md.ClusterConfigs.BOOTSTRAP_SERVERS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
final class ProducingTest
{
    @Value
    @Builder
    private static class Availability
    {
        public int replicationFactor;
        public int minInsyncReplicas;
        @NonNull
        public String settingsName;
    }

    @Value
    @Builder
    private static class Params
    {
        @NonNull
        public Availability availability;
        public int numPartitions;
        @NonNull
        public String acks;

        public NewTopic newTopic()
        {
            return new NewTopic(
                    format("%s_acks-%s_pts-%d", availability.settingsName, acks, numPartitions),
                    numPartitions,
                    (short) availability.replicationFactor).
                    configs(of(
                            "min.insync.replicas", Integer.toString(availability.minInsyncReplicas)
                            )
                    );
        }
    }

    private static Stream<Params> params()
    {
        final Availability balanced = Availability.builder().
                replicationFactor(3).
                minInsyncReplicas(2).
                settingsName("balanced").
                build();
        final Availability low = Availability.builder().
                replicationFactor(1).
                minInsyncReplicas(1).
                settingsName("low").
                build();
        final Availability max = Availability.builder().
                replicationFactor(3).
                minInsyncReplicas(1).
                settingsName("max").
                build();
        final Availability moderate = Availability.builder().
                replicationFactor(2).
                minInsyncReplicas(1).
                settingsName("moderate").
                build();

        return Stream.of(balanced, low, max, moderate).flatMap(availability ->
                Stream.of(1, 2, 3).flatMap(numPartitions ->
                        Stream.of("0", "1", "all").map(acks ->
                                Params.builder().
                                        availability(availability).
                                        numPartitions(numPartitions).
                                        acks(acks).
                                        build()
                        )
                )
        );
    }

    @ParameterizedTest
    @MethodSource("params")
    void test(final Params params) throws ExecutionException, InterruptedException
    {
        final AdminClient admin = createAdmin();
        final NewTopic newTopic = params.newTopic();
        admin.createTopics(singletonList(newTopic)).all().get();

        final int start = 0;
        final int end = 1_000_000;
        produce(start, end, newTopic, params.acks);
        consume(start, end, newTopic);

        admin.deleteTopics(singletonList(newTopic.name())).all().get();
        admin.close();
    }

    private static AdminClient createAdmin()
    {
        final Map<String, Object> adminConfig = ImmutableMap.<String, Object>builder().
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterConfigs.BOOTSTRAP_SERVERS).
                build();

        return AdminClient.create(adminConfig);
    }

    private static Producer<String, String> createProducer(final String acks)
    {
        final Map<String, Object> producerConfig = ImmutableMap.<String, Object>builder().
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(ACKS_CONFIG, acks).
                build();
        return new KafkaProducer<>(producerConfig);
    }

    private static Consumer<String, String> createConsumer()
    {
        final Map<String, Object> consumerConfig = ImmutableMap.<String, Object>builder().
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()).
                put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()).
                put(GROUP_ID_CONFIG, "test-group").
                put(AUTO_OFFSET_RESET_CONFIG, "earliest").
                build();
        return new KafkaConsumer<>(consumerConfig);
    }

    private static void produce(final int start, final int end, final NewTopic newTopic, @NonNull String acks)
    {
        final Producer<String, String> producer = createProducer(acks);
        for (int i = start; i < end; i++)
        {
            final int index = i;
            final ProducerRecord<String, String> record = new ProducerRecord<>(newTopic.name(), "key", Integer.toString(index));
            producer.send(record, (metadata, exception) -> {
                        if (nonNull(exception))
                            log.warn("Error while sending record {}", index);
                    }
            );
        }
        producer.flush();
        producer.close();
    }

    private static void consume(final int start, final int end, final NewTopic newTopic)
    {
        final Consumer<String, String> consumer = createConsumer();
        consumer.subscribe(singletonList(newTopic.name()));
        int expected = start;
        while (expected < end)
        {
            final ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));
            for (final ConsumerRecord<String, String> record : records)
            {
                final int actual = Integer.parseInt(record.value());
                if (expected != actual)
                {
                    log.warn("Miss: exp {}, act {} [{}]", expected, actual, newTopic.name());
                    expected = actual;
                }
                expected++;
            }
        }
        consumer.close();
    }
}
