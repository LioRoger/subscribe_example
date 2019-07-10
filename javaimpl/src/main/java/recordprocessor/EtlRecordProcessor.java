package recordprocessor;


import com.alibaba.dts.formats.avro.Record;
import common.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordgenerator.OffsetCommitCallBack;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static common.Util.require;
import static common.Util.sleepMS;


/**
 * This demo show how to resolve avro record deserialize from bytes
 * We will show how to print a column from deserialize record
 */
public class EtlRecordProcessor implements  Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(EtlRecordProcessor.class);
    private final OffsetCommitCallBack offsetCommitCallBack;
    private volatile Checkpoint commitCheckpoint;
    private WorkThread commitThread;
    public boolean offer(long timeOut, TimeUnit timeUnit, ConsumerRecord record) {
        try {
            return toProcessRecord.offer(record, timeOut, timeUnit);
        } catch (Exception e) {
            log.error("EtlRecordProcessor: offer record failed, record[" + record + "], cause " + e.getMessage(), e);
            return false;
        }
    }

    private final LinkedBlockingQueue<ConsumerRecord> toProcessRecord;
    private final AvroDeserializer fastDeserializer;
    private final Context context;
    private final Map<String, RecordListener> recordListeners = new HashMap<>();

    private volatile boolean existed = false;
    public EtlRecordProcessor(OffsetCommitCallBack offsetCommitCallBack, Context context) {
        this.offsetCommitCallBack = offsetCommitCallBack;
        this.toProcessRecord = new LinkedBlockingQueue<>(512);
        fastDeserializer = new AvroDeserializer();
        this.context = context;
        commitCheckpoint = new Checkpoint(null, -1, -1, "-1");
        commitThread = getCommitThread();
        commitThread.start();
    }


    @Override
    public void run() {
        while (!existed) {
            ConsumerRecord<byte[], byte[]> toProcess = null;
            Record record = null;
            int fetchFailedCount = 0;
            try {
                while (null == (toProcess = toProcessRecord.peek()) && !existed) {
                    sleepMS(5);
                    fetchFailedCount++;
                    if (fetchFailedCount % 1000 == 0) {
                        log.info("EtlRecordProcessor: haven't receive records from generator for  5s");
                    }
                }
                if (existed) {
                    return;
                }
                fetchFailedCount = 0;
                final ConsumerRecord<byte[], byte[]> consumerRecord = toProcess;
                record = fastDeserializer.deserialize(consumerRecord.value());
                log.debug("EtlRecordProcessor: meet [{}] record type", record.getOperation());
                for (RecordListener recordListener : recordListeners.values()) {
                    recordListener.consume(new UserRecord(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(), record, new UserCommitCallBack() {
                        @Override
                        public void commit(TopicPartition tp, Record commitRecord, long offset, String metadata) {
                            commitCheckpoint = new Checkpoint(tp, commitRecord.getSourceTimestamp(), offset, metadata);
                        }
                    }));
                }
                toProcessRecord.poll();
            } catch (Exception e) {
                log.error("EtlRecordProcessor: process record failed, raw consumer record [" + toProcess + "], parsed record [" + record + "], cause " + e.getMessage(), e);
                existed = true;
            }
        }
    }

    // user define how to commit
    private void commit() {
        if (null != offsetCommitCallBack) {
            if (commitCheckpoint.getTopicPartition() != null && commitCheckpoint.getOffset() != -1) {
                log.info("commit record with checkpoint {}", commitCheckpoint);
                offsetCommitCallBack.commit(commitCheckpoint.getTopicPartition(), commitCheckpoint.getTimeStamp(),
                        commitCheckpoint.getOffset(), commitCheckpoint.getInfo());
            }
        }
    }

    public void registerRecordListener(String name, RecordListener recordListener) {
        require(null != name && null != recordListener, "null value not accepted");
        recordListeners.put(name, recordListener);
    }

    public  void close() {
        this.existed = true;
        commitThread.stop();
    }

    private WorkThread getCommitThread() {
        WorkThread workThread = new WorkThread(new Runnable() {
            @Override
            public void run() {
                while (!existed) {
                    sleepMS(5000);
                    commit();
                }
            }
        });
        return workThread;
    }

}
