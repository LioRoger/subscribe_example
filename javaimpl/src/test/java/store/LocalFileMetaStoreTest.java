package store;

import metastore.LocalFileMetaStore;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import common.Checkpoint;
import common.Util;

import static org.junit.Assert.assertTrue;

public class LocalFileMetaStoreTest {
    @Test
    public void testLocalFileStore() {
        String fileName = "fileStore";
        String desc = "";
        Util.deleteFile(fileName);
        LocalFileMetaStore localFileStore = new LocalFileMetaStore(fileName);
        assertTrue(null == localFileStore.deserializeFrom(new TopicPartition("t1", 0), "xxx"));
        localFileStore.serializeTo(new TopicPartition("t1", 0), "aa", new Checkpoint(new TopicPartition("t1", 0), 11, 11, ""));
        localFileStore.serializeTo(new TopicPartition("t2", 0), "bb", new Checkpoint(new TopicPartition("t1", 0), 22, 22, ""));
        localFileStore.serializeTo(new TopicPartition("t2", 1), "bb", new Checkpoint(new TopicPartition("t1", 0), 44, 44, ""));

        localFileStore.serializeTo(new TopicPartition("t1", 0), "aa", new Checkpoint(new TopicPartition("t1", 0), 33, 33, ""));
        assertTrue(null == localFileStore.deserializeFrom(new TopicPartition("t1", 0), "xxx"));
        Checkpoint aaCheckpoint = localFileStore.deserializeFrom(new TopicPartition("t1", 0), "aa");
        assertTrue(null != aaCheckpoint && aaCheckpoint.getOffset() == 33 && aaCheckpoint.getTimeStamp() == 33);
        Checkpoint bbCheckpoint = localFileStore.deserializeFrom(new TopicPartition("t2", 0), "bb");
        assertTrue(null != bbCheckpoint && bbCheckpoint.getOffset() == 22 && bbCheckpoint.getTimeStamp() == 22);

        // init another store
        LocalFileMetaStore anotherStore = new LocalFileMetaStore(fileName);
        assertTrue(null == anotherStore.deserializeFrom(new TopicPartition("t1", 0), "xxx"));
        aaCheckpoint = anotherStore.deserializeFrom(new TopicPartition("t1", 0), "aa");
        assertTrue(null != aaCheckpoint && aaCheckpoint.getOffset() == 33 && aaCheckpoint.getTimeStamp() == 33);
        bbCheckpoint = anotherStore.deserializeFrom(new TopicPartition("t2", 0), "bb");
        assertTrue(null != bbCheckpoint && bbCheckpoint.getOffset() == 22 && bbCheckpoint.getTimeStamp() == 22);
        Checkpoint ccCheckpoint = anotherStore.deserializeFrom(new TopicPartition("t2", 1), "bb");
        assertTrue(null != ccCheckpoint && ccCheckpoint.getOffset() == 44 && ccCheckpoint.getTimeStamp() == 44);
        Util.deleteFile(fileName);
    }
}
