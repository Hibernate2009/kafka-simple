package ru.alfastrah.interplat.bus.kafka.delivery;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class DeliveryProcessingApp {

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


        Serializer<Delivery> deliverySerializer = new DeliverySerializer();
        Deserializer<Delivery> deliveryDeserializer = new DeliveryDeserializer();
        Serde<Delivery> deliverySerde = Serdes.serdeFrom(deliverySerializer, deliveryDeserializer);

        Serializer<DeliveryAggregator> deliveryAggSerializer = new DeliveryAggregatorSerializer();
        Deserializer<DeliveryAggregator> deliveryAggDeserializer = new DeliveryAggregatorDeserializer();
        Serde<DeliveryAggregator> deliveryAggSerde = Serdes.serdeFrom(deliveryAggSerializer, deliveryAggDeserializer);

        final Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, DeliveryAggregator> incomingMessageStream = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, deliveryAggSerde));
        incomingMessageStream.flatMapValues(value -> value.logs)
        .filter(new Predicate<String, Delivery>() {
            @Override
            public boolean test(String key, Delivery value) {
                boolean result = false;
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                try {
                    Date parseDate = simpleDateFormat.parse(value.time);
                    Date currentDate = new Date();
                    result =  currentDate.after(parseDate);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return result;
            }
        })
        .to(OUTPUT_TOPIC, Produced.valueSerde(deliverySerde));


        Topology build = builder.build();

        final KafkaStreams streams = new KafkaStreams(build, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
