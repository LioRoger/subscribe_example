package boot;

import common.RecordListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import recordgenerator.OffsetCommitCallBack;
import recordprocessor.EtlRecordProcessor;
import recordgenerator.RecordGenerator;
import recordgenerator.ConsumerWrapFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;
import common.Checkpoint;
import common.Context;
import common.WorkThread;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static recordgenerator.Names.*;
import static common.Util.*;

public class Boot {
    private static final Logger log = LoggerFactory.getLogger(Boot.class);
    private static final AtomicBoolean existed = new AtomicBoolean(false);


    public static void boot(String configFile, Map<String, RecordListener> recordListeners) {
        boot(loadConfig(configFile), recordListeners);
    }

    public static void boot(Properties properties, Map<String, RecordListener> recordListeners) {
        // first init log4j
        initLog4j();
        require(null != recordListeners && !recordListeners.isEmpty(), "record listener required");
        Context context = getStreamContext(properties);
        // check config
        checkConfig(properties);
        RecordGenerator recordGenerator = getRecordGenerator(context, properties);
        EtlRecordProcessor etlRecordProcessor = getEtlRecordProcessor(context, properties, recordGenerator);
        recordListeners.forEach((k, v) -> {
            log.info("Boot: register record listener " + k);
            etlRecordProcessor.registerRecordListener(k, v);
        });
        registerSignalHandler(context);
        List<WorkThread> startStream = startWorker(etlRecordProcessor, recordGenerator);

        while (!existed.get() ) {
            sleepMS(1000);
        }
        log.info("StreamBoot: shutting down...");
        for (WorkThread workThread : startStream) {
            workThread.stop();
        }

    }

    private static List<WorkThread> startWorker(EtlRecordProcessor etlRecordProcessor, RecordGenerator recordGenerator) {
        List<WorkThread> ret = new LinkedList<>();
        ret.add(new WorkThread(etlRecordProcessor));
        ret.add(new WorkThread(recordGenerator));
        for (WorkThread workThread : ret) {
            workThread.start();
        }
        return ret;
    }

    private static void registerSignalHandler(Context context) {
        SignalHandler signalHandler = new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                // SIG_INT
                if (signal.getNumber() == 2) {
                    existed.compareAndSet(false, true);
                }
            }
        };
        Signal.handle(new Signal("INT"), signalHandler);
    }

    private static Context getStreamContext(Properties properties) {
        Context ret =  new Context();
        return ret;
    }


    // offset@timestamp or timestamp
    private static Checkpoint parseCheckpoint(String checkpoint) {
        require(null != checkpoint, "checkpoint should not be null");
        String[] offsetAndTS = checkpoint.split("@");
        Checkpoint streamCheckpoint = null;
        if (offsetAndTS.length == 1) {
            streamCheckpoint =  new Checkpoint(null, Long.valueOf(offsetAndTS[0]), -1, "");
        } else if (offsetAndTS.length >= 2) {
            streamCheckpoint =  new Checkpoint(null, Long.valueOf(offsetAndTS[0]), Long.valueOf(offsetAndTS[1]), "");
        }
        return streamCheckpoint;
    }

    private static RecordGenerator getRecordGenerator(Context context, Properties properties) {

        RecordGenerator recordGenerator = new RecordGenerator(properties, context,
                parseCheckpoint(properties.getProperty(INITIAL_CHECKPOINT_NAME)),
                new ConsumerWrapFactory.KafkaConsumerWrapFactory());
        context.setStreamSource(recordGenerator);
        return recordGenerator;
    }

    private static EtlRecordProcessor getEtlRecordProcessor(Context context, Properties properties, RecordGenerator recordGenerator) {

        EtlRecordProcessor etlRecordProcessor = new EtlRecordProcessor(new OffsetCommitCallBack() {
            @Override
            public void commit(TopicPartition tp, long timestamp, long offset, String metadata) {
                recordGenerator.setToCommitCheckpoint(new Checkpoint(tp, timestamp, offset, metadata));
            }
        }, context);
        context.setRecordProcessor(etlRecordProcessor);
        return etlRecordProcessor;
    }


    // may check some fo config value
    private static void checkConfig(Properties properties) {
        require(null != properties.getProperty(USER_NAME), "use should supplied");
        require(null != properties.getProperty(PASSWORD_NAME), "password should supplied");
        require(null != properties.getProperty(SID_NAME), "sid should supplied");
        require(null != properties.getProperty(KAFKA_TOPIC), "kafka topic should supplied");
        require(null != properties.getProperty(KAFKA_BROKER_URL_NAME), "broker url should supplied");
    }

    private static Properties initLog4j() {
        Properties properties = new Properties();
        InputStream log4jInput = null;
        try {
            log4jInput = Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties");
            PropertyConfigurator.configure(log4jInput);
        } catch (Exception e) {
        } finally {
            swallowErrorClose(log4jInput);
        }
        return properties;
    }


    private static Properties loadConfig(String filePath) {
        Properties ret = new Properties();
        InputStream toLoad = null;
        try {
            toLoad = new BufferedInputStream(new FileInputStream(filePath));
            ret.load(toLoad);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }  finally {
            swallowErrorClose(toLoad);
        }
        return ret;
    }
}
