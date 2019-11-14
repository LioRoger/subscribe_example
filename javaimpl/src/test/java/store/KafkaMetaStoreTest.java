package store;

import metastore.KafkaMetaStore;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import common.Checkpoint;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaMetaStoreTest {

    public KafkaConsumer getTestKafkaConsumer(String groupID) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.101.175.161:8085");
        //props.put("auto.commit.interval.ms", "1000");
        // props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("group.id", groupID);
        return new KafkaConsumer(props);
    }

    @Test
    public void testKafkaStore() throws ExecutionException, InterruptedException {
        String groupID = "xxxxx";
        TopicPartition tp = new TopicPartition("dts_kafka_topic03", 0);
        KafkaConsumer kafkaConsumer = getTestKafkaConsumer(groupID);
        kafkaConsumer.assign(Arrays.asList(tp));
        KafkaMetaStore kafkaStore = new KafkaMetaStore(kafkaConsumer);
        Future f = kafkaStore.serializeTo(tp, groupID, new Checkpoint(tp, 2048, 2048, ""));

        //
        kafkaConsumer.poll(1000);
        f.get();
     //   sleepMS(10000);
        Checkpoint checkpoint = kafkaStore.deserializeFrom(tp, groupID);
        System.out.println(checkpoint);
    }
}
