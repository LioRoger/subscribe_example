import common.RecordListener;
import boot.MysqlRecordPrinter;
import common.UserRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static boot.Boot.boot;
import static recordgenerator.Names.*;

public class NotifyDemo {
    private static final Logger log = LoggerFactory.getLogger(NotifyDemo.class);

    public static Map<String, RecordListener> buildRecordListener() {
        // user can impl their own listener
        RecordListener mysqlRecordPrintListener = new RecordListener() {
            @Override
            public void consume(UserRecord record) {
                // consume record
                // MysqlRecordPrinter show how to go through record fields and get general attributes
                String ret = MysqlRecordPrinter.recordToString(record.getRecord());
        //        log.info(ret);
                record.commit(String.valueOf(record.getRecord().getSourceTimestamp()));
            }
        };
        return Collections.singletonMap("mysqlRecordPrinter", mysqlRecordPrintListener);
    }

    /**
     * This demo use  hard coded config. User can modify variable value for test
     * The detailed describe for var in resources/demoConfig
     */
    public static Properties getConfigs() {
        Properties properties = new Properties();
        // user password and sid for auth
        properties.setProperty(USER_NAME, "1k1r9ljx74j76");
        properties.setProperty(PASSWORD_NAME, "1iey9qsu3kvoi");
        properties.setProperty(SID_NAME, "qeexktltmbtu1");
        // kafka consumer group general same with sid
        properties.setProperty(GROUP_NAME, "qeexktltmbtu1");
        // topic to consume, partition is 0
        properties.setProperty(KAFKA_TOPIC, "cn_shanghai_11.161.33.41_3306_root");
        // kafka broker url
        properties.setProperty(KAFKA_BROKER_URL_NAME, "11.163.186.193:17013");
        // initial checkpoint for first seek
        properties.setProperty(INITIAL_CHECKPOINT_NAME, "1560977122");
        // if force use config checkpoint when start. for checkpoint reset
        properties.setProperty(USE_CONFIG_CHECKPOINT_NAME, "false");
        // use consumer assign or subscribe interface
        // when use subscribe mode, group config is required. kafka consumer group is enabled
        properties.setProperty(SUBSCRIBE_MODE_NAME, "assign");
        return properties;
    }

    public static void main(String[] args) throws InterruptedException {

        try{
            boot(getConfigs(), buildRecordListener());
        }catch(Throwable e){
            log.error("NotifyDemo: failed cause " + e.getMessage(), e);
            throw e;
        } finally {
            System.exit(0);
        }
    }
}
