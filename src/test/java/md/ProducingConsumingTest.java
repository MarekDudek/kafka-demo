package md;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
final class ProducingConsumingTest
{
    private static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092";
    private static final String TOPIC = "test-topic";
    private static final String KEY = "key";
    private static final String VALUE = "value";


    @Test
    void single_record_is_produced_and_consumed() throws ExecutionException, InterruptedException
    {
        final Map<String, Object> adminConfig = ImmutableMap.<String, Object>builder().
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                build();
        final AdminClient admin = AdminClient.create(adminConfig);

        admin.createTopics(singletonList(new NewTopic(TOPIC, empty(), empty()))).all().get();

        final Map<String, Object> producerConfig = ImmutableMap.<String, Object>builder().
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(ACKS_CONFIG, "all").
                build();
        final Producer<String, String> producer = new KafkaProducer<>(producerConfig);

        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, KEY, VALUE);
        final RecordMetadata md = producer.send(producerRecord).get();
        producer.flush();
        assertThat(md.hasOffset()).isTrue();
        assertThat(md.hasTimestamp()).isTrue();
        producer.close();

        final Map<String, Object> consumerConfig = ImmutableMap.<String, Object>builder().
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()).
                put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()).
                put(GROUP_ID_CONFIG, "test-group").
                put(AUTO_OFFSET_RESET_CONFIG, "earliest").
                build();
        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);

        consumer.subscribe(singletonList(TOPIC));
        final ConsumerRecords<String, String> records = consumer.poll(ofSeconds(1));
        assertThat(records.count()).isEqualTo(1);
        final ConsumerRecord<String, String> consumerRecord = records.iterator().next();
        assertThat(consumerRecord.key()).isEqualTo(KEY);
        assertThat(consumerRecord.value()).isEqualTo(VALUE);
        consumer.close();

        admin.deleteTopics(singletonList(TOPIC)).all().get();
    }
}