package kafka.producer.async;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by ggchangan on 17-6-29.
 */
public class ASyncProducer {
    static final String topic = "topicTest";
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("acks", "all");
        //异步生产者和同步生产者的区别，这里不能设置acks属性，同时设置producer.type属性
        //TODO 测试同步生产者和异步生产者的性能
        props.put("producer.type", "async");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "kafka.producer.partition.SimplePartitioner");

        Producer<String, String> producer = new KafkaProducer<>(props);
        long events = Long.MAX_VALUE;
        Random rnd = new Random();
        for (long nEvents = 0; nEvents < events; nEvents ++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.baidu.com," + ip;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, ip, msg);
            producer.send(record);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
