package ru.alfastrah.interplat.bus.kafka.delivery.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DeliveryDeserializer implements Deserializer<Delivery> {

    private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Delivery deserialize(String s, byte[] bytes) {
        return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), Delivery.class);
    }

    @Override
    public void close() {

    }
}
