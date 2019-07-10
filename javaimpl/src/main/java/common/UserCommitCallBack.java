package common;

import com.alibaba.dts.formats.avro.Record;
import org.apache.kafka.common.TopicPartition;

public interface UserCommitCallBack {
    public void commit(TopicPartition tp, Record record, long offset, String metadata);
}
