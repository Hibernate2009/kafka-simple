package ru.alfastrah.interplat.bus.kafka.delivery;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;
import org.apache.kafka.streams.state.WindowStore;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.DeliveryAggregatorDeserializer;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.DeliveryAggregatorSerializer;

import java.time.Duration;
import java.util.Properties;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class DeliveryAggregatorApp {
    public static final String APPLICATION_ID = "delivery-aggregator";
    public static final String INPUT_TOPIC = "messages";
    public static final String OUTPUT_TOPIC = "delivery-output-stream";

    public String bootstrapServers;

    public DeliveryAggregatorApp(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9092";
        DeliveryAggregatorApp deliveryAggregatorApp = new DeliveryAggregatorApp(bootstrapServers);
        deliveryAggregatorApp.run();
    }


    protected void run() {
        Properties streamsConfig = buildStreamsConfig(bootstrapServers);
        StreamsBuilder streamsBuilder = configureStreamsBuilder(new StreamsBuilder());

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
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

        Serializer<DeliveryAggregator> deliveryAggSerializer = new DeliveryAggregatorSerializer();
        Deserializer<DeliveryAggregator> deliveryAggDeserializer = new DeliveryAggregatorDeserializer();
        Serde<DeliveryAggregator> deliveryAggSerde = Serdes.serdeFrom(deliveryAggSerializer, deliveryAggDeserializer);

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();

        SessionWindowedSerializer<String> windowedSerializer = new SessionWindowedSerializer<>(stringSerializer);
        SessionWindowedDeserializer<String> windowedDeserializer = new SessionWindowedDeserializer<>(stringDeserializer);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        KStream<String, String> inputStream = streamsBuilder.stream(INPUT_TOPIC);

        long windowSizeMs = 60 * 1000L;

        inputStream
                .groupBy(new DeliveryKeyValueMapper())
                .windowedBy(TimeWindows.of(ofMinutes(2)).advanceBy(ofMinutes(1)))
                //.windowedBy(TimeWindows.of(windowSizeMs))
                .aggregate(
                        DeliveryAggregator::new,
                        (key, value, deliveryAgg) -> deliveryAgg.add(value),
                        Materialized.<String, DeliveryAggregator, WindowStore<Bytes, byte[]>>
                                as("delivery-input-stream-aggregated")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(deliveryAggSerde)

                )
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(windowedSerde, deliveryAggSerde));

        return streamsBuilder;
    }
}
