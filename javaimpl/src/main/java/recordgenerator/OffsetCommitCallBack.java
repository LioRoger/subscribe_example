package recordgenerator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public interface OffsetCommitCallBack {
    void commit(TopicPartition tp, long timestamp, long offset, String metadata);
}
