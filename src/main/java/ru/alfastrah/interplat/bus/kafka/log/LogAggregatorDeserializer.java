package ru.alfastrah.interplat.bus.kafka.log;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class LogAggregatorDeserializer implements Deserializer<LogAggregator> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public LogAggregator deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return new LogAggregator(bytes);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing value", e);
        }

    }

}