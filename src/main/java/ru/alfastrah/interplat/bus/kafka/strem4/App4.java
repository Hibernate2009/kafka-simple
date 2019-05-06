package ru.alfastrah.interplat.bus.kafka.strem4;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.KStreamAggProcessorSupplier;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class App4 {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> incomingMessageStream = builder.stream("messages", Consumed.with(stringSerde, stringSerde));



        KStream<String, String> stream =
                builder.stream("messages", Consumed.with(Serdes.String(), Serdes.String()));



        incomingMessageStream
                .groupBy((key, word) -> word, Grouped.with(
                        Serdes.String(), /* key (note: type was modified) */
                        Serdes.String()) )
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        new Initializer<String>() { /* initializer */
                            @Override
                            public String apply() {
                                return "";
                            }
                        },
                        new Aggregator<String, String, String>() { /* adder */
                            @Override
                            public String apply(String aggKey, String newValue, String aggValue) {
                                System.out.println(aggValue + newValue);
                                return aggValue + newValue;
                            }
                        },
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                                .withValueSerde(Serdes.String()))
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
