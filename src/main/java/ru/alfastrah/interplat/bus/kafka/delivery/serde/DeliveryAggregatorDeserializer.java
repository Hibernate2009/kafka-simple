package ru.alfastrah.interplat.bus.kafka.delivery.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import ru.alfastrah.interplat.bus.kafka.delivery.DeliveryAggregator;
import ru.alfastrah.interplat.bus.kafka.log.LogAggregator;

import java.util.Map;

public class DeliveryAggregatorDeserializer implements Deserializer<DeliveryAggregator> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public DeliveryAggregator deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return new DeliveryAggregator(bytes);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing value", e);
        }
    }

    @Override
    public void close() {

    }
}
