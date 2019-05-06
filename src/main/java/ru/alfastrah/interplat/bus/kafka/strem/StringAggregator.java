package ru.alfastrah.interplat.bus.kafka.strem;

import org.apache.kafka.streams.kstream.Aggregator;

import java.util.ArrayList;
import java.util.List;

public class StringAggregator implements Aggregator<String, String, StringAggregator>{

    private List<String> list = new ArrayList<>();


    public void add(String str){
        list.add(str);
    }

    @Override
    public StringAggregator apply(String key, String value, StringAggregator aggregate) {
        return null;
    }
}
