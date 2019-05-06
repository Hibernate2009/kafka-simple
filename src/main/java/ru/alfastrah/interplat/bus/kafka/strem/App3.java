package ru.alfastrah.interplat.bus.kafka.strem;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.state.SessionStore;
import ru.alfastrah.interplat.bus.kafka.log.LogAggregator;
import ru.alfastrah.interplat.bus.kafka.log.LogAggregatorDeserializer;
import ru.alfastrah.interplat.bus.kafka.log.LogAggregatorSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMinutes;

public class App3 {
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


        Serializer<LogAggregator> logAggSerializer = new LogAggregatorSerializer();
        Deserializer<LogAggregator> logAggDeserializer = new LogAggregatorDeserializer();
        Serde<LogAggregator> logAggSerde = Serdes.serdeFrom(logAggSerializer, logAggDeserializer);
        Materialized<String, LogAggregator, SessionStore<Bytes, byte[]>> materialized = Materialized.<String, LogAggregator, SessionStore<Bytes, byte[]>>
                as("log-input-stream-aggregated")
                .withKeySerde(Serdes.String())
                .withValueSerde(logAggSerde);

        List<String> list = new ArrayList<>();

        KGroupedStream<String, String> groupedStream = incomingMessageStream.groupBy(
                (key, value) -> value,
                Grouped.with(
                        Serdes.String(), /* key (note: type was modified) */
                        Serdes.String())  /* value */
        );

        KTable kTable = applyAggregate(groupedStream, new StringAggregatorInitializer(), new StringAggregator(), materialized);
        kTable.toStream().print(Printed.toSysOut());


        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @SuppressWarnings("unchecked")
    public static KTable applyAggregate(KGroupedStream groupedStream,
                                 Initializer initializer,
                                 Aggregator aggregator,
                                 Materialized<String, LogAggregator, ?> materialized) {
        final TimeWindows windows = TimeWindows.of(ofMinutes(1)).grace(ofMinutes(1));
        return groupedStream.windowedBy(windows).aggregate(initializer, aggregator, materialized);
    }
}
