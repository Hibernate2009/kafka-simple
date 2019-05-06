package ru.alfastrah.interplat.bus.kafka;

import java.util.ArrayList;

public class MessageAggregator {
    private ArrayList<MessageEntry> messages = new ArrayList<>();

    public MessageAggregator add(String message){
        messages.add(new MessageEntry(message));
        return this;
    }
}
