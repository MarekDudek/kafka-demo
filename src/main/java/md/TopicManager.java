package md;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;

@AllArgsConstructor
@Slf4j
public final class TopicManager
{
    private final AdminClient admin;

    public void createTopic(final NewTopic newTopic) throws ExecutionException, InterruptedException
    {
        final CreateTopicsResult result = admin.createTopics(singletonList(newTopic));
        final KafkaFuture<Void> all = result.all();
        all.get();
    }

    public void deleteTopic(final String topic) throws ExecutionException, InterruptedException
    {
        final DeleteTopicsResult result = admin.deleteTopics(singletonList(topic));
        final KafkaFuture<Void> all = result.all();
        all.get();
    }

    public void close()
    {
        admin.close();
    }
}
