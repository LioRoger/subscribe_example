package common;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import recordgenerator.ClusterSwitchListener;

import java.io.Closeable;
import java.io.File;
import java.util.Properties;

import static recordgenerator.Names.*;

public class Util {
    public static  void swallowErrorClose(Closeable target) {
        try {
            if (null != target) {
                target.close();
            }
        } catch (Exception e) {
        }
    }

    public static void sleepMS(long value) {
        try {
            Thread.sleep(value);
        } catch (Exception e) {
        }
    }

    public static void mergeSourceKafkaProperties(Properties originProperties, Properties mergeToProperties) {
        originProperties.forEach((k, v) ->{
            String key = (String)k;
            if (key.startsWith("kafka.")) {
                String toPutKey = key.substring(6);
                mergeToProperties.setProperty(toPutKey, (String)v);
            }
        });
        mergeToProperties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                buildJaasConfig(originProperties.getProperty(SID_NAME), originProperties.getProperty(USER_NAME), originProperties.getProperty(PASSWORD_NAME)));
        mergeToProperties.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
        mergeToProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        mergeToProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, originProperties.getProperty(KAFKA_BROKER_URL_NAME));
        mergeToProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, originProperties.getProperty(GROUP_NAME));
        // disable auto commit
        mergeToProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        mergeToProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        mergeToProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // to let the consumer feel the switch of cluster and reseek the offset by timestamp
        mergeToProperties.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ClusterSwitchListener.class.getName());

    }

    public static void require(boolean predict, String errMessage) {
        if (!predict) {
            throw new RuntimeException(errMessage);
        }
    }

    public static String buildJaasConfig(String sid, String  user, String password) {
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s-%s\" password=\"%s\";";
        return String.format(jaasTemplate, user, sid,  password);
    }

    public static boolean checkFileExists(String fileName) {
        File metaFile = new File(fileName);
        return metaFile.exists();
    }

    public static void deleteFile(String fileName) {
        File maybeAbsolutePath = new File(fileName);
        if (!maybeAbsolutePath.exists()) {
            return;
        }
        File metaFile = null;
        if (maybeAbsolutePath.isAbsolute()) {
            metaFile = maybeAbsolutePath;
        } else {
            File currentPath = new File(".");
            metaFile = new File(currentPath.getAbsolutePath() + File.separator + fileName);
        }
        boolean deleted = metaFile.delete();
        if (!deleted) {
            throw new RuntimeException(metaFile.getAbsolutePath() + " should be cleaned anyway");
        }
    }

    private static void setIfAbsent(Properties properties, String key, String valueSetIfAbsent) {
        if (StringUtils.isEmpty(properties.getProperty(key))) {
            properties.setProperty(key, valueSetIfAbsent);
        }
    }

    public  static String[] uncompressionObjectName(String compressionName){
        if(null == compressionName || compressionName.isEmpty() ){
            return  null;
        }

        String [] names = compressionName.split("\\.");

        int length = names.length;

        for(int i=0;i<length;++i){
            names[i] = unescapeName(names[i]);
        }
        return names;
    }

    private  static String unescapeName(String name){
        if (null == name || (name.indexOf("\\u002E") < 0)) {
            return name;
        }

        StringBuilder builder = new StringBuilder();

        int length = name.length();

        for(int i=0;i<length;++i){
            char c = name.charAt(i);
            if('\\' == c && ( i<length-6 && 'u' == name.charAt(i + 1)
                    && '0' == name.charAt(i + 2) && '0' == name.charAt(i+3)
                    && '2' == name.charAt(i + 4) && 'E' == name.charAt(i+5))){
                builder.append(".");
                i += 5;
                continue;
            }else{
                builder.append(c);
            }
        }

        return builder.toString();
    }


    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {
        }
    }
}
