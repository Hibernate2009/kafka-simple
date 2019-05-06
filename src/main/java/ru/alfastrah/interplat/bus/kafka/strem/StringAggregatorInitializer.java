package ru.alfastrah.interplat.bus.kafka.strem;

import org.apache.kafka.streams.kstream.Initializer;

import java.util.ArrayList;
import java.util.List;

public class StringAggregatorInitializer implements Initializer<StringAggregatorInitializer> {
    List<String> list = new ArrayList<>();
    @Override
    public StringAggregatorInitializer apply() {
        return this;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }
}
