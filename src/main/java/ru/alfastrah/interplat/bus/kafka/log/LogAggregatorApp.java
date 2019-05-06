package ru.alfastrah.interplat.bus.kafka.log;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class LogAggregatorApp {
    public static final String APPLICATION_ID = "log-aggregator";
    public static final String INPUT_TOPIC = "messages";
    public static final String OUTPUT_TOPIC = "log-output-stream";

    public String bootstrapServers;
    public Topology topology;
    public KafkaStreams streams;
    public Properties streamsConfig;

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9092";
        LogAggregatorApp logAggregatorApp = new LogAggregatorApp(bootstrapServers);
        logAggregatorApp.build();
        logAggregatorApp.run();
    }

    public LogAggregatorApp(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    protected void build() {
        streamsConfig = buildStreamsConfig(bootstrapServers);
        StreamsBuilder streamsBuilder = configureStreamsBuilder(new StreamsBuilder());

        this.topology = streamsBuilder.build();
        this.streams = new KafkaStreams(topology, streamsConfig);
    }

    protected void run() {
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    protected Properties buildStreamsConfig(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }

    protected StreamsBuilder configureStreamsBuilder(StreamsBuilder streamsBuilder) {

       // 1 minute session, go ahead and change this
        final Long inactivityGap = TimeUnit.MINUTES.toMillis(1);

        Serializer<LogAggregator> logAggSerializer = new LogAggregatorSerializer();
        Deserializer<LogAggregator> logAggDeserializer = new LogAggregatorDeserializer();
        Serde<LogAggregator> logAggSerde = Serdes.serdeFrom(logAggSerializer, logAggDeserializer);

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();

        SessionWindowedSerializer<String> windowedSerializer = new SessionWindowedSerializer<>(stringSerializer);
        SessionWindowedDeserializer<String> windowedDeserializer = new SessionWindowedDeserializer<>(stringDeserializer);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        KStream<String, String> inputStream = streamsBuilder.stream(INPUT_TOPIC);

       /* inputStream
                .groupBy((key, value) -> value)
                .windowedBy(SessionWindows.with(Duration.ofSeconds(15)))
                .aggregate(
                        LogAggregator::new,
                        (key, value, logAgg) -> logAgg.add(value),
                        (key, loggAgg1, logAgg2) -> new LogAggregator(loggAgg1, logAgg2),
                        Materialized.<String, LogAggregator, SessionStore<Bytes, byte[]>>
                                as("log-input-stream-aggregated")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(logAggSerde)
                )
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(windowedSerde, logAggSerde));*/

        inputStream
                .groupBy((key, value) -> value)
                .windowedBy(TimeWindows.of(ofSeconds(10)).advanceBy(ofSeconds(10)))
                .aggregate(
                        LogAggregator::new,
                        (key, value, logAgg) -> logAgg.add(value),
                        Materialized.<String, LogAggregator, WindowStore<Bytes, byte[]>>
                                 as("log-input-stream-aggregated")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(logAggSerde)

                )
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(windowedSerde, logAggSerde));



        return streamsBuilder;

    }
}
