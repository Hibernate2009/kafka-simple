package ru.alfastrah.interplat.bus.kafka.log;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class LogAggregatorSerializer implements Serializer<LogAggregator> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public byte[] serialize(String topic, LogAggregator logAgg) {
        if (logAgg == null) {
            return null;
        }

        try {
            return logAgg.asByteArray();
        } catch (RuntimeException e) {
            throw new SerializationException("Error serializing value", e);
        }

    }

}
