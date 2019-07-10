package common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordgenerator.RecordGenerator;
import recordprocessor.EtlRecordProcessor;

public class Context {
    private static final Logger log = LoggerFactory.getLogger(Context.class);
    private RecordGenerator streamSource;
    private EtlRecordProcessor recordProcessor;
    public void setStreamSource(RecordGenerator streamSource) {
        this.streamSource = streamSource;
    }

    public EtlRecordProcessor getRecordProcessor() {
        return recordProcessor;
    }

    public  void setRecordProcessor(EtlRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

}
