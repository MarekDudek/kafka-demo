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

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static md.ClusterConfigs.BOOTSTRAP_SERVERS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;


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

        final int produceCount = 1_000_000;
        produce(produceCount, newTopic, params.acks);
        final BitSet consumed = consume(produceCount, newTopic);

        admin.deleteTopics(singletonList(newTopic.name())).all().get();
        admin.close();

        final int consumedCount = consumed.cardinality();
        assertThat(consumedCount).
                as("Missing %d on topic %s", produceCount - consumedCount, newTopic.name()).
                isEqualTo(produceCount);
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

    private static void produce(final int count, final NewTopic newTopic, final String acks)
    {
        final Producer<String, String> producer = createProducer(acks);
        for (int i = 0; i < count; i++)
        {
            final ProducerRecord<String, String> record = new ProducerRecord<>(newTopic.name(), Integer.toString(i));
            producer.send(record, (metadata, exception) -> {
                        if (nonNull(exception))
                        {
                            producer.close();
                            fail("Error while sending record", exception);
                        }
                    }
            );
        }
        producer.flush();
        producer.close();
    }

    private static BitSet consume(final int count, final NewTopic newTopic)
    {
        final Consumer<String, String> consumer = createConsumer();
        consumer.subscribe(singletonList(newTopic.name()));
        final BitSet consumed = new BitSet(count);
        final ConsumerRecords<String, String> first = consumer.poll(ofSeconds(5));
        for (final ConsumerRecord<String, String> record : first)
            consumed.set(parseInt(record.value()));
        while (true)
        {
            final ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));
            if (records.isEmpty())
                break;
            for (final ConsumerRecord<String, String> record : records)
                consumed.set(parseInt(record.value()));
        }
        consumer.close();
        return consumed;
    }
}
