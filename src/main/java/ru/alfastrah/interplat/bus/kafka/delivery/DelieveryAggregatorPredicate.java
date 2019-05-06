package ru.alfastrah.interplat.bus.kafka.delivery;

import org.apache.kafka.streams.kstream.Predicate;

public class DelieveryAggregatorPredicate implements Predicate<String, DeliveryAggregator> {
    @Override
    public boolean test(String key, DeliveryAggregator value) {

        return false;
    }
}
