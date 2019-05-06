package ru.alfastrah.interplat.bus.kafka.delivery.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DeliverySerializer implements Serializer<Delivery> {

    private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Delivery delivery) {
        return gson.toJson(delivery).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
}
