package kafka.consume.partition;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;

import static java.util.stream.Collectors.toList;


/**
 * Created by ggchangan on 17-6-26.
 */
public class PartitionConsumer {
    private List<String> replicBrokers = new ArrayList<>();

    public static void main(String[] args) {
        final String topic = "topicTest";

        if (args.length < 1) {
            System.out.println("Please assign partition number.");
        }

        List<String> brokers = new ArrayList<>();
        brokers.add("172.24.65.159");

        final int port = 9092;

        PartitionConsumer consumer = new PartitionConsumer();
        long maxReads = Long.MAX_VALUE;

        int partLen = Integer.parseInt(args[0]);
        for (int index = 0; index < partLen; index++) {
            try {
                consumer.run(maxReads, topic, index, brokers, port);
            } catch (Exception e) {
                System.out.println("Oops:" + e);
                e.printStackTrace();
            }
        }

    }

    private void run(long maxReads, String topic, int partition, List<String> brokers, int port) throws Exception {
        // find the meta data about the topic and partition we are interested in
        PartitionMetadata metadata = findLeader(brokers, port, topic, partition);

        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return;
        }

        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + topic + "_" + partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

        int numErrors = 0;
        while (maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            }

            kafka.api.FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();

            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, port);
                continue;
            }
            numErrors = 0;

            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                numRead++;
                maxReads--;
            }

            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null) consumer.close();
    }

    //TODO 为什么需要遍历所有的brokers
    private PartitionMetadata findLeader(List<String> brokers, int port, String topic, int partition) {
        PartitionMetadata exactPartitionMetadata = null;

        for (String broker: brokers) {
            SimpleConsumer consumer = null;

            try{
                consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, "leaderLookup");

                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = consumer.send(req);

                List<kafka.javaapi.TopicMetadata> topicMetadatas=  resp.topicsMetadata();
                for (TopicMetadata topicMetadata: topicMetadatas) {
                    for (PartitionMetadata partitionMetadata: topicMetadata.partitionsMetadata()) {
                        if (partitionMetadata.partitionId() == partition) {
                            exactPartitionMetadata = partitionMetadata;
                            break;
                        }
                    }
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }

        if (exactPartitionMetadata != null) {
            replicBrokers.clear();
            replicBrokers.addAll(exactPartitionMetadata.replicas().stream().map(x->x.host()).collect(toList()));
        }

        return exactPartitionMetadata;
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicBrokers, port, topic, partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

}
