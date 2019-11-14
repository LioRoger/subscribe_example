package metastore;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.Future;

public interface MetaStore<V> {
    Future<V> serializeTo(TopicPartition topicPartition, String group, V value);
    V deserializeFrom(TopicPartition topicPartition, String group);
}
