package recordgenerator;

import java.util.Properties;

public interface ConsumerWrapFactory {
    public ConsumerWrap getConsumerWrap(Properties properties);

    public static class KafkaConsumerWrapFactory implements ConsumerWrapFactory {
        @Override
        public ConsumerWrap getConsumerWrap(Properties properties) {
            return new ConsumerWrap.DefaultConsumerWrap(properties);
        }
    }
}
