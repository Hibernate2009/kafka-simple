package ru.alfastrah.interplat.bus.kafka.delivery;

import com.google.gson.Gson;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.Delivery;

public class DeliveryKeyValueMapper implements KeyValueMapper<String, String, String> {
    Gson gson = new Gson();

    @Override
    public String apply(String key, String value) {
        Delivery delivery = gson.fromJson(value, Delivery.class);
        return delivery.queue;
    }
}
