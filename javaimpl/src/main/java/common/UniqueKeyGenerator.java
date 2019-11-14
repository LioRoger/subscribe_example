package common;

import java.util.concurrent.atomic.AtomicLong;
/**
 * for compaction enabled topic, empty key field in producer record is not allowed, so we gene random key to avoid compaction
 */
public class UniqueKeyGenerator {
    private AtomicLong counter;
    private final String startMSStr;
    public UniqueKeyGenerator() {
        counter = new AtomicLong(0);
        startMSStr = String.valueOf(System.currentTimeMillis()) + "-";
    }
    public String nextKey() {
        return startMSStr + counter.getAndIncrement();
    }

}