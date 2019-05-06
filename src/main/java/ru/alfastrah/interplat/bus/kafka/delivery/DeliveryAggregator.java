package ru.alfastrah.interplat.bus.kafka.delivery;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.Delivery;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DeliveryAggregator {
    ArrayList<Delivery> logs = new ArrayList<>();
    Gson gson = new Gson();

    public DeliveryAggregator() {
    }

    public DeliveryAggregator(String jsonString) {
        ArrayList<Delivery> logEntries = gson.fromJson(jsonString, new TypeToken<List<Delivery>>() {
        }.getType());
        logs.addAll(logEntries);
    }

    public DeliveryAggregator(byte[] bytes) {
        this(new String(bytes));
    }

    public DeliveryAggregator add(String log) {
        Delivery logEntry = gson.fromJson(log, Delivery.class);
        logs.add(logEntry);
        return this;
    }

    public String asJsonString() {
        return gson.toJson(logs);
    }

    public byte[] asByteArray() {
        return asJsonString().getBytes(StandardCharsets.UTF_8);
    }
}
