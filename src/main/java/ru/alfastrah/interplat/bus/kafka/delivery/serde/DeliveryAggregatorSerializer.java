package ru.alfastrah.interplat.bus.kafka.delivery.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import ru.alfastrah.interplat.bus.kafka.delivery.DeliveryAggregator;

import java.util.Map;

public class DeliveryAggregatorSerializer implements Serializer<DeliveryAggregator> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, DeliveryAggregator deliveryAggregator) {
        if (deliveryAggregator == null) {
            return null;
        }

        try {
            return deliveryAggregator.asByteArray();
        } catch (RuntimeException e) {
            throw new SerializationException("Error serializing value", e);
        }
    }

    @Override
    public void close() {

    }
}
