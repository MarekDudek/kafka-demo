package md;


import com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.nonNull;
import static md.ClusterConfigs.BOOTSTRAP_SERVERS;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
final class MultipleProducersTest
{

    @Test
    void test() throws InterruptedException
    {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.submit(new MyProducer(0, 100, 'k', 5));
        executor.submit(new MyProducer(300, 1000, 'l', 4));
        executor.submit(new MyProducer(2000, 10000, 'm', 30));

        Sleep.sleep(Duration.ofSeconds(5));

        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.SECONDS))
            executor.shutdownNow();
    }
}

@Slf4j
@RequiredArgsConstructor
class MyProducer implements Runnable
{
    private final int start;
    private final int end;
    private final char prefix;
    private final int wrap;

    @Override
    public void run()
    {
        final Map<String, Object> config = ImmutableMap.<String, Object>builder().
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS).
                put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()).
                put(ACKS_CONFIG, "all").
                build();
        final KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        for (int i = start; i < end; i++)
        {
            final String key = String.format("%c-%d", prefix, i % wrap);
            final String value = Integer.toString(i);
            final ProducerRecord<String, String> record = new ProducerRecord<>("shared-topic", key, value);
            producer.send(record, (metadata, exception) -> {
                        if (nonNull(exception))
                            log.error("Error while sending record for {}: {}", metadata, exception.getCause());
                    }
            );
        }
        producer.flush();
        producer.close();
    }
}