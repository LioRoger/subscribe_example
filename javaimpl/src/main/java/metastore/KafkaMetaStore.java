package metastore;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import common.Checkpoint;


import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

public class KafkaMetaStore implements MetaStore<Checkpoint> {
    private static final Logger log = LoggerFactory.getLogger(KafkaMetaStore.class);

    private volatile KafkaConsumer kafkaConsumer;

    public KafkaMetaStore(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public void resetKafkaConsumer(KafkaConsumer newConsumer) {
        this.kafkaConsumer = newConsumer;
    }

    @Override
    public Future<Checkpoint> serializeTo(TopicPartition topicPartition, String group, Checkpoint value) {
        KafkaFutureImpl ret = new KafkaFutureImpl();
        if (null != kafkaConsumer) {
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(value.getOffset(), String.valueOf(value.getTimeStamp()));
            // Notice: commitAsync is only put commit offset request to sending queue, the future  result will be driven by KafkaConsumer.poll() function
            // So if you only call this method but not poll, you may not wait offset commit call back
            kafkaConsumer.commitAsync(Collections.singletonMap(topicPartition, offsetAndMetadata), new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (null != exception) {
                        log.warn("KafkaMetaStore: Commit offset for group[" + group + "] topicPartition[" + topicPartition.toString() + "] " +
                                value.toString() + " failed cause " + exception.getMessage(), exception);
                        ret.completeExceptionally(exception);
                    } else {
                        log.debug("KafkaMetaStore:Commit offset success for group[{}] topicPartition [{}] {}", group, topicPartition, value);
                        ret.complete(value);
                    }
                }
            });
        } else {
            log.warn("KafkaMetaStore: kafka consumer not set, ignore report");
            ret.complete(value);
        }
        return ret;

    }

    @Override
    public Checkpoint deserializeFrom(TopicPartition topicPartition, String group) {
        if (null != kafkaConsumer) {
            OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(topicPartition);
            if (null != offsetAndMetadata) {
                return new Checkpoint(topicPartition, Long.valueOf(offsetAndMetadata.metadata()), offsetAndMetadata.offset(), offsetAndMetadata.metadata());
            } else {
                return null;
            }
        } else {
            log.warn("KafkaMetaStore: kafka consumer not set, ignore fetch offset");
            throw new KafkaException("KafkaMetaStore: kafka consumer not set, ignore fetch offset for group[" + group + "] and tp [" + topicPartition + "]");
        }
    }
}
