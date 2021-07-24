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
import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static md.ClusterConfigs.BOOTSTRAP_SERVERS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
final class ProducingTest
{
    private static final String SERIALIZER = StringSerializer.class.getName();
    private static final String DESERIALIZER = StringDeserializer.class.getName();
    private static final int NUM_PARTITIONS = 1;

    @Value
    @Builder
    private static class TopicParams
    {
        public int numPartitions;
        public int replicationFactor;
        public int minInsyncReplicas;
        @NonNull
        public String name;
    }

    @Value
    @Builder
    private static class Params
    {
        @NonNull
        public TopicParams topicParams;
        @NonNull
        public String acks;

        public NewTopic newTopic()
        {
            return new NewTopic(
                    topicParams.name + "-" + acks,
                    topicParams.numPartitions,
                    (short) topicParams.replicationFactor).
                    configs(of(
                            "min.insync.replicas", Integer.toString(topicParams.minInsyncReplicas)
                            )
                    );
        }
    }

    private static Stream<Params> params()
    {
        final TopicParams balanced = TopicParams.builder().
                numPartitions(1).
                replicationFactor(3).
                minInsyncReplicas(2).
                name("balanced").
                build();
        final TopicParams low = TopicParams.builder().
                numPartitions(1).
                replicationFactor(1).
                minInsyncReplicas(1).
                name("low").
                build();
        final TopicParams max = TopicParams.builder().
                numPartitions(1).
                replicationFactor(3).
                minInsyncReplicas(1).
                name("max").
                build();
        final TopicParams moderate = TopicParams.builder().
                numPartitions(1).
                replicationFactor(2).
                minInsyncReplicas(1).
                name("moderate").
                build();

        final String zero = "0";
        final String one = "1";
        final String all = "all";

        return Stream.of(balanced, low, max, moderate).flatMap(topicParams ->
                Stream.of(zero, one, all).flatMap(acks ->
                        Stream.of(
                                Params.builder().
                                        topicParams(topicParams).
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
        final int end = 1_0_000;
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
                put(KEY_SERIALIZER_CLASS_CONFIG, SERIALIZER).
                put(VALUE_SERIALIZER_CLASS_CONFIG, SERIALIZER).
                put(ACKS_CONFIG, acks).
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

    private static void produce(final int start, final int end, final NewTopic newTopic, @NonNull String acks)
    {
        final Producer<String, String> producer = createProducer(acks);
        for (int i = start; i < end; i++)
        {
            final int index = i;
            final ProducerRecord<String, String> record = new ProducerRecord<>(newTopic.name(), "key", Integer.toString(index));
            producer.send(record, (metadata, exception) -> {
                        if (nonNull(exception))
                            log.error("Error while sending record #{}", index);
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
        int next = start;
        while (next < end)
        {
            final ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));
            for (final ConsumerRecord<String, String> record : records)
            {
                final int current = Integer.parseInt(record.value());
                if (current != next)
                    log.error("Missing at, current = {}, next = {}", current, next);
                next++;
            }
        }
        consumer.close();
    }
}
