package kafka.consumer.group;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ggchangan on 17-6-23.
 */
public class GroupConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;


    public GroupConsumer(String zookeeper, String group, String topic) {
        ConsumerConfig config = createConsumerConfig(zookeeper, group);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        this.topic = topic;
    }

    private ConsumerConfig createConsumerConfig(String zookeeper, String group) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", group);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public static void main(String[] args) {
        //final String zookeeper = "172.24.63.51:2181,172.24.63.52:2181";
        final String zookeeper = "172.24.65.159:2181";
        String group = "groupTest" + new Random().nextInt(1000);
        final String topic = "topicTest";

        int threads = Integer.parseInt(args[0]);

        String groupInfo = String.format("Start consumer group:%s with %d consumers", group, threads, topic);
        System.out.println(groupInfo);

        GroupConsumer groupConsumer = new GroupConsumer(zookeeper, group, topic);

        groupConsumer.start(threads);

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
        }

        groupConsumer.stop();
    }

    public void stop() {
        if (consumer != null) {
            consumer.shutdown();
        }

        if (executor != null) {
            executor.shutdown();
        }

        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                System.out.println("Time out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }

    }

    /**
     * 一个组里面消费者实例个数
     * @param threads
     */
    public void start(int threads) {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(threads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap= consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(threads);

        int threadNumber = 0;
        for (final  KafkaStream stream: streams) {
            executor.submit(new Cunsumer(stream, threadNumber));
            threadNumber ++;
        }

    }
}
