package ru.alfastrah.interplat.bus.kafka.log;

import com.google.gson.Gson;

public class LogEntry {

    public String message;



    public LogEntry(String message) {
        this.message = message;
    }

    public static LogEntry fromJson(String jsonString) {
        LogEntry logEntry = new Gson().fromJson(jsonString, LogEntry.class);
        return logEntry;
    }

    public String asJsonString() {
        return new Gson().toJson(this);
    }
}