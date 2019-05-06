package ru.alfastrah.interplat.bus.kafka.stream2;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class App4 {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> strings = Serdes.String();
        KStream<String, String> messages = builder.stream("messages", Consumed.with(strings, strings));


        KGroupedStream<String, String> groupedStream = messages.groupBy(
                (key, value) -> value  /* value */
        );

        TimeWindowedKStream<String, String> stringStringTimeWindowedKStream = groupedStream.windowedBy(TimeWindows.of(Duration.ofMinutes(5)));
        KTable<Windowed<String>, String> timeWindowedAggregatedStream = stringStringTimeWindowedKStream
                .aggregate(
                        new Initializer<String>() { /* initializer */
                            @Override
                            public String apply() {
                                return "";
                            }
                        },
                        new Aggregator<String, String, String>() {
                            public String apply(String key, String newValue, String aggValue) {
                                return aggValue + newValue;
                            }
                        },
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                                .withValueSerde(Serdes.String()));

        timeWindowedAggregatedStream.suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream()
                .print(Printed.toSysOut());


        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
