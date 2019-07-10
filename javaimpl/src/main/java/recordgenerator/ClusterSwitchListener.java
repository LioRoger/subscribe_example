package recordgenerator;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *  We recommend user register this listener.
 *  Cause when origin cluster is unavailable and new cluster is created by HA(high available service).
 *  The cluster name is different. We want warn user that a new cluster is working.
 *  The more important thing is that we want user recreate KakfaConsumer and use timestamp to reseek offset.
 *  If user following this guid, less duplicated data will be pushed.
 *  Otherwise
 */
public class ClusterSwitchListener implements ClusterResourceListener, ConsumerInterceptor {
    private final static Logger logger = LoggerFactory.getLogger(ClusterSwitchListener.class);
    private ClusterResource originClusterResource = null;

    public ConsumerRecords onConsume(ConsumerRecords records) {
        return records;
    }


    public void close() {
    }

    public void onCommit(Map offsets) {
    }


    public void onUpdate(ClusterResource clusterResource) {
        synchronized (this) {
            if (null == originClusterResource) {
                logger.info("Cluster updated to " + clusterResource.clusterId());
                originClusterResource = clusterResource;
            } else {
                if (clusterResource.clusterId().equals(originClusterResource.clusterId())) {
                    logger.info("Cluster not changed on update:" + clusterResource.clusterId());
                } else {
                    throw new ClusterSwitchException("Cluster changed from " + originClusterResource.clusterId() + " to " + clusterResource.clusterId()
                            + ", consumer require restart");
                }
            }
        }
    }

    public void configure(Map<String, ?> configs) {
    }

    public static class ClusterSwitchException extends KafkaException {
        public ClusterSwitchException(String message, Throwable cause) {
            super(message, cause);
        }

        public ClusterSwitchException(String message) {
            super(message);
        }

        public ClusterSwitchException(Throwable cause) {
            super(cause);
        }

        public ClusterSwitchException() {
            super();
        }

    }
}

