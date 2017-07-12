package kafka.zookeeper;

import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.zookeeper.data.Stat;
import scala.Option;
import scala.Tuple2;

import java.util.*;

/**
 * Created by ggchangan on 17-7-6.
 */
public class ZookeeperInfoFetcher {
    private final int SESSION_TIMEOUT = Integer.MAX_VALUE;
    private final int CONNECTION_TIMEOUT = 1000;
    private int sessionTimeout = SESSION_TIMEOUT;
    private int connectionTimeout = CONNECTION_TIMEOUT;
    private String zkUrl;

    public ZookeeperInfoFetcher(String zkQuorum) {
        zkUrl = zkQuorum;
    }

    public List<String> listBrokers() {
        //return zkClient.getChildren("/brokers/ids");

        return new ArrayList<>();
    }

    public Integer findLeader(String topic, int partition) {
        Tuple2<ZkClient, ZkConnection> zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout);
        ZkUtils zkUtils = new ZkUtils(zkClientAndConnection._1(), zkClientAndConnection._2(), false);
        Option<Object> leaderId = zkUtils.getLeaderForPartition(topic, partition);
        return (Integer) leaderId.get();
    }

    public void findBroker(int brokerId) {
        Tuple2<ZkClient, ZkConnection> zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout);
        ZkUtils zkUtils = new ZkUtils(zkClientAndConnection._1(), zkClientAndConnection._2(), false);
        /*
        List<Broker> brokers = (List<Broker>) zkUtils.getAllBrokersInCluster().toList();
        for (Broker broker: brokers) {
        }
        */

        Broker broker = zkUtils.getBrokerInfo(brokerId).get();
        BrokerEndPoint brokerEndPoint = broker.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT));
        System.out.println(brokerEndPoint.id());
        //输出每个broker的id
        System.out.println(brokerEndPoint.host());
        System.out.println(brokerEndPoint.port());
    }

    public long getOffsetFromZooKeeper(String groupId, String topic, int partition) {
        TopicAndPartition tap = new TopicAndPartition(topic, partition);
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, tap.topic());

        Tuple2<ZkClient, ZkConnection> zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout);
        ZkUtils zkUtils = new ZkUtils(zkClientAndConnection._1(), zkClientAndConnection._2(), false);
        scala.Tuple2<Option<String>, Stat> data = zkUtils.readDataMaybeNull(
                topicDirs.consumerOffsetDir() + "/" + tap.partition());

        final long OFFSET_NOT_SET = 0;
        if (data._1().isEmpty()) {
            return OFFSET_NOT_SET;
        } else {
            return Long.valueOf(data._1().get());
        }
    }

    /**
     * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
     * @param groupID consumer group to get offsets for
     * @param topic topic to get offsets for
     * @return mapping of (topic and) partition to offset
     */
    /*
    public static Map<TopicAndPartition,Long> getOffsets(String zkServers,
                                                         String groupID,
                                                         String topic) {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupID, topic);
        Map<TopicAndPartition,Long> offsets = new HashMap<>();
        try (AutoZkClient zkClient = new AutoZkClient(zkServers)) {
            List<Object> partitions = JavaConversions.seqAsJavaList(
                    ZkUtils.getPartitionsForTopics(
                            zkClient,
                            JavaConversions.asScalaBuffer(Collections.singletonList(topic))).head()._2());
            for (Object partition : partitions) {
                String partitionOffsetPath = topicDirs.consumerOffsetDir() + "/" + partition;
                Option<String> maybeOffset = ZkUtils.readDataMaybeNull(zkClient, partitionOffsetPath)._1();
                Long offset = maybeOffset.isDefined() ? Long.parseLong(maybeOffset.get()) : null;
                TopicAndPartition topicAndPartition =
                        new TopicAndPartition(topic, Integer.parseInt(partition.toString()));
                offsets.put(topicAndPartition, offset);
            }
        }
        return offsets;
    }
    */


    public static void main(String[] args) {
        String zkQuorum = "172.24.8.111";
        final String topic = "ds-7-rs-11-1498790016000";
        ZookeeperInfoFetcher fetcher = new ZookeeperInfoFetcher(zkQuorum);
        int partition = 0;
        Integer leaderId = fetcher.findLeader(topic, partition);
        assert leaderId == 12;
        fetcher.findBroker(leaderId);
        System.out.println(leaderId == 12);
        partition= 1;
        leaderId = fetcher.findLeader(topic, partition);
        assert leaderId == 11;
        System.out.println(leaderId == 11);
        fetcher.findBroker(leaderId);

    }
}
