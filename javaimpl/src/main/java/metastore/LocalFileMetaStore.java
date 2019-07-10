package metastore;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import common.Checkpoint;
import common.AtomicFileStore;

import java.util.*;
import java.util.concurrent.Future;

public class LocalFileMetaStore implements MetaStore<Checkpoint> {
    private static final String GROUP_ID_NAME = "groupID";
    private static final String STREAM_CHECKPOINT_NAME = "streamCheckpoint";
    private static final String TOPIC_NAME = "topic";
    private static final String PARTITION_NAME = "partition";
    private static final String OFFSET_NAME = "offset";
    private static final String TIMESTAMP_NAME = "timestamp";
    private static final String INFO_NAME = "info";

    private static class StoreElement {
        final String groupName;
        final Map<TopicPartition, Checkpoint> streamCheckpoint;

        private StoreElement(String groupName, Map<TopicPartition, Checkpoint> streamCheckpoint) {
            this.groupName = groupName;
            this.streamCheckpoint = streamCheckpoint;
        }
    }

    private final AtomicFileStore fileStore;
    private final Map<String, Map<TopicPartition, Checkpoint>> inMemStore = new HashMap<>();
    public LocalFileMetaStore(String fileName) {
        this.fileStore = new AtomicFileStore(fileName);
    }

    private String toJson(StoreElement storeElement) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(GROUP_ID_NAME, storeElement.groupName);
        JSONArray jsonArray = new JSONArray();
        storeElement.streamCheckpoint.forEach((tp, checkpoint) -> {
            JSONObject streamCheckpointJsonObject = new JSONObject();
            streamCheckpointJsonObject.put(TOPIC_NAME, tp.topic());
            streamCheckpointJsonObject.put(PARTITION_NAME, tp.partition());
            streamCheckpointJsonObject.put(OFFSET_NAME, checkpoint.getOffset());
            streamCheckpointJsonObject.put(TIMESTAMP_NAME, checkpoint.getTimeStamp());
            streamCheckpointJsonObject.put(INFO_NAME, checkpoint.getInfo());
            jsonArray.add(streamCheckpointJsonObject);
        });

        jsonObject.put(STREAM_CHECKPOINT_NAME, jsonArray);
        return jsonObject.toJSONString();
    }

    private StoreElement fromString(String jsonString) {
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        String groupName = jsonObject.getString(GROUP_ID_NAME);
        JSONArray streamCheckpointJsonObject = jsonObject.getJSONArray(STREAM_CHECKPOINT_NAME);
        Map<TopicPartition, Checkpoint> checkpointInfo = new HashMap<>();
        for (Object o : streamCheckpointJsonObject) {
            JSONObject tpAndCheckpoint = (JSONObject) o;
            String topic = tpAndCheckpoint.getString(TOPIC_NAME);
            int partition = tpAndCheckpoint.getInteger(PARTITION_NAME);
            long offset = tpAndCheckpoint.getLong(OFFSET_NAME);
            long timestamp = tpAndCheckpoint.getLong(TIMESTAMP_NAME);
            String info = tpAndCheckpoint.getString(INFO_NAME);
            checkpointInfo.put(new TopicPartition(topic, partition), new Checkpoint(new TopicPartition(topic, partition), timestamp, offset, info));
        }

        return new StoreElement(groupName, checkpointInfo);
    }

    @Override
    public Future<Checkpoint> serializeTo(TopicPartition topicPartition, String groupID, Checkpoint value) {
        Map<TopicPartition, Checkpoint> topicPartitionCheckpoint = inMemStore.get(groupID);
        if (null == topicPartitionCheckpoint) {
            topicPartitionCheckpoint = new HashMap<>();
        }
        topicPartitionCheckpoint.put(topicPartition, value);
        inMemStore.put(groupID, topicPartitionCheckpoint);

        List<String> toSerialize = new LinkedList<>();
        inMemStore.forEach((k, v) ->{
            toSerialize.add(toJson(new StoreElement(k, v)));
        });
        fileStore.updateContent(toSerialize);
        KafkaFutureImpl ret =  new KafkaFutureImpl<>();
        ret.complete(value);
        return ret;
    }

    @Override
    public Checkpoint deserializeFrom(TopicPartition topicPartition, String groupID) {
        Map<TopicPartition, Checkpoint> tpAndCheckpointMap = inMemStore.get(groupID);
        if (null != tpAndCheckpointMap) {
            Checkpoint ret = tpAndCheckpointMap.get(topicPartition);
            if (null != ret) {
                return ret;

            }
        }
        List<String> storedCheckpoint = fileStore.getContent();
        for (String checkpoint : storedCheckpoint) {
            StoreElement storeElement = fromString(checkpoint);
            // add to cache
            if (!inMemStore.containsKey(storeElement.groupName)) {
                inMemStore.put(storeElement.groupName, storeElement.streamCheckpoint);
            }
            if (StringUtils.equals(storeElement.groupName, groupID)) {
                return storeElement.streamCheckpoint.get(topicPartition);
            }
        }
        return null;
    }

}
