package kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

/**
 * it is designed to operate on an infinite, unbounded stream of data. Similar to the bounded variant, it is a stateful
 * algorithm that tracks and updates the counts of words. However, since it must assume potentially unbounded input data.
 * it will periodically output its current state and results while continuing to process more data because it cannot know
 * when it has processed "all" the input data
 *
 *
 * Kafka Streams is doing here is to leverage the duality between a table and a changelog stream(here: table = the KTable,
 * changelog stream = the downstream KStream): you can publish every change of the table to stream, and if you consume the
 * entire changelog stream from beginning to end, you can reconstruct the contents of the table
 */
public class WordCountApplication {
    public static void main(String[] args) throws InterruptedException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "magneto7020:9092");
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        //config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("TextLinesTopic");
        KTable<String, Long> wordCounts = textLines.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count("Counts");

        wordCounts.print(Serdes.String(), Serdes.Long());
        wordCounts.to(Serdes.String(), Serdes.Long(), "WordsWithCountsTopic");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

}
