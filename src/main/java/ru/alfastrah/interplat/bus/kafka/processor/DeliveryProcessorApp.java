package ru.alfastrah.interplat.bus.kafka.processor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.Delivery;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.DeliveryDeserializer;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.DeliverySerializer;

import java.util.Properties;

public class DeliveryProcessorApp {

    public static final String INPUT_TOPIC = "delivery-output-stream";
    public static final String OUTPUT_TOPIC = "delivery-result-stream";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "delivery-processing");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Serializer<ru.alfastrah.interplat.bus.kafka.delivery.serde.Delivery> deliverySerializer = new DeliverySerializer();
        Deserializer<ru.alfastrah.interplat.bus.kafka.delivery.serde.Delivery> deliveryDeserializer = new DeliveryDeserializer();
        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        Serde<Delivery> deliverySerde = Serdes.serdeFrom(deliverySerializer, deliveryDeserializer);
        final Serde<String> stringSerde = Serdes.String();


        StoreBuilder<KeyValueStore<String, Delivery>> deliveryStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("Deliveries"),
                Serdes.String(),
                deliverySerde)
                .withLoggingDisabled();


        Topology builder = new Topology();
        builder.addSource("Source", stringDeserializer, deliveryDeserializer, INPUT_TOPIC)
                .addProcessor("Process", () -> new DeliveryProcessor(), "Source")
                .addStateStore(deliveryStore, "Process")
                .addSink("Sink", OUTPUT_TOPIC,stringSerializer, deliverySerializer, "Process");


        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
