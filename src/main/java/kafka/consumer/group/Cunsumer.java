package kafka.consumer.group;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.util.concurrent.Callable;

/**
 * Created by ggchangan on 17-6-23.
 */
public class Cunsumer implements Runnable {
    private KafkaStream stream;
    private int threadNumber;

    public Cunsumer(KafkaStream stream, int threadNumber) {
        this.stream = stream;
        this.threadNumber = threadNumber;
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> consumerIter = stream.iterator();

        while (consumerIter.hasNext()) {
            String msg = String.format("Thread %d: %s", threadNumber, new String(consumerIter.next().message()));
            System.out.println(msg);
        }

        System.out.println(String.format("Shutdown thread: %d", threadNumber));
    }
}
