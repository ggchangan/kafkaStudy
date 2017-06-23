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
        ConsumerIterator consumerIter = stream.iterator();

        while (consumerIter.hasNext()) {
            /*
            MessageAndMetadata messageAndMetadata = consumerIter.next();

            String key = (String) messageAndMetadata.key();

            String value = (String) messageAndMetadata.message();
            String msg = String.format("Thread %Id: %s", threadNumber, key+":"+value);
            */
            /*
            MessageAndMetadata kafkaMsg = (MessageAndMetadata) consumerIter.next().message();
            System.out.println(kafkaMsg.key());
            System.out.println(kafkaMsg.message());
            */

            String msg = String.format("Thread %Id: %s", threadNumber, consumerIter.next().message().toString());
            System.out.println(msg);
        }

        System.out.println(String.format("Shutdown thread: %d", threadNumber));
    }
}
