package ru.alfastrah.interplat.bus.kafka.log;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.streams.kstream.Aggregator;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class LogAggregator {
    ArrayList<LogEntry> logs = new ArrayList<>();
    Gson gson = new Gson();

    public LogAggregator() {
    }

    public LogAggregator(LogAggregator logAgg1, LogAggregator logAgg2) {
        logs.addAll(logAgg1.logs);
        logs.addAll(logAgg2.logs);
    }

    public LogAggregator(String jsonString) {
        ArrayList<LogEntry> logEntries = gson.fromJson(jsonString, new TypeToken<List<LogEntry>>() {
        }.getType());
        logs.addAll(logEntries);
    }

    public LogAggregator(byte[] bytes) {
        this(new String(bytes));
    }

    public LogAggregator add(String log) {
        LogEntry logEntry = gson.fromJson(log, LogEntry.class);
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

