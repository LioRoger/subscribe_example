package recordgenerator;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import common.Checkpoint;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static recordgenerator.Names.*;
import static common.Util.mergeSourceKafkaProperties;

public abstract class ConsumerWrap implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerWrap.class);

    // directly set offset using the give offset, we don't check the offset is legal or not.
    public abstract void setFetchOffsetByOffset(TopicPartition topicPartition, Checkpoint checkpoint);
    public abstract void setFetchOffsetByTimestamp(TopicPartition topicPartition, Checkpoint checkpoint);
    // assign topic is not use auto balance, which we recommend this way to consume record. and commit offset by user it self
    public abstract void assignTopic(TopicPartition topicPartition, Checkpoint checkpoint);
    // subscribe function use consumer group mode, which means multi consumer using the same groupid could build a high available consume system
    // still we recommend shutdown auto commit mode, and user commit the offset manually.
    // this can delay offset commit until the record is really consumed by business logic which can strongly defend the data loss.
    public abstract void subscribeTopic(TopicPartition topicPartition, Supplier<Checkpoint> streamCheckpoint);


    public abstract ConsumerRecords<byte[], byte[]> poll();

    public abstract KafkaConsumer getRawConsumer();

    public static class DefaultConsumerWrap extends ConsumerWrap {
        private AtomicBoolean firstStart = new AtomicBoolean(true);
        private KafkaConsumer<byte[], byte[]> consumer;
        private final long poolTimeOut;

        public DefaultConsumerWrap(Properties properties) {
            Properties consumerConfig = new Properties();
            mergeSourceKafkaProperties(properties, consumerConfig);
            checkConfig(consumerConfig);
            consumer = new KafkaConsumer<byte[], byte[]>(consumerConfig);
            poolTimeOut = Long.valueOf(properties.getProperty(POLL_TIME_OUT, "500"));
        }

        @Override
        public void setFetchOffsetByOffset(TopicPartition topicPartition, Checkpoint checkpoint) {
            consumer.seek(topicPartition, checkpoint.getOffset());
        }

        // recommended
        @Override
        public void setFetchOffsetByTimestamp(TopicPartition topicPartition, Checkpoint checkpoint) {
            long timeStamp = checkpoint.getTimeStamp();
            Map<TopicPartition, OffsetAndTimestamp> remoteOffset = consumer.offsetsForTimes(Collections.singletonMap(topicPartition, timeStamp));
            OffsetAndTimestamp toSet = remoteOffset.get(topicPartition);
            if (null == toSet) {
                throw new RuntimeException("RecordGenerator:seek timestamp for topic [" + topicPartition + "] with timestamp [" + timeStamp + "] failed");
            }
            consumer.seek(topicPartition, toSet.offset());
        }

        @Override
        public void assignTopic(TopicPartition topicPartition, Checkpoint checkpoint) {
            consumer.assign(Arrays.asList(topicPartition));
            log.info("RecordGenerator:  assigned for {} with checkpoint {}", topicPartition, checkpoint);
            setFetchOffsetByTimestamp(topicPartition, checkpoint);
        }


        //Not test, please not use this function
        @Override
        public void subscribeTopic(TopicPartition topicPartition, Supplier<Checkpoint> streamCheckpoint) {
            consumer.subscribe(Arrays.asList(topicPartition.topic()), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    log.info("RecordGenerator: partition revoked for [{}]", StringUtils.join(partitions, ","));
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    log.info("RecordGenerator: partition assigned for [{}]", StringUtils.join(partitions, ","));
                    if (partitions.contains(topicPartition)) {
                        if (firstStart.compareAndSet(true, false)) {
                            Checkpoint toSet = streamCheckpoint.get();
                            setFetchOffsetByTimestamp(topicPartition, toSet);
                            log.info("RecordGenerator:  subscribe for [{}] with checkpoint [{}] first start", topicPartition, toSet);
                        } else {
                            log.info("RecordGenerator:  subscribe for [{}]  reassign, do nothing", topicPartition);
                        }
                    }
                }
            });
        }

        public ConsumerRecords<byte[], byte[]> poll() {
            return consumer.poll(poolTimeOut);
        }

        @Override
        public KafkaConsumer getRawConsumer() {
            return consumer;
        }

        public synchronized void close() {
            if (null != consumer) {
                consumer.close();
            }
        }

        private void checkConfig(Properties properties) {

        }

    }
}
