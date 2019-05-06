package ru.alfastrah.interplat.bus.kafka.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import ru.alfastrah.interplat.bus.kafka.delivery.serde.Delivery;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class DeliveryProcessor extends AbstractProcessor <String, Delivery>{
    private ProcessorContext context;
    private KeyValueStore<String, Delivery> deliveryStore;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        deliveryStore = (KeyValueStore) context.getStateStore("Deliveries");

        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
            KeyValueIterator<String, Delivery> iter = this.deliveryStore.all();
            while (iter.hasNext()) {
                KeyValue<String, Delivery> entry = iter.next();
                boolean result = isResult(entry);
                if (result) {
                   context.forward(entry.key, entry.value);
                   deliveryStore.delete(entry.key);
                }
            }
            iter.close();

            // commit the current processing progress
            context.commit();
        });
    }

    private boolean isResult(KeyValue<String, Delivery> entry) {
        boolean result = false;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        try {
            Date parseDate = simpleDateFormat.parse(entry.value.time);
            Date currentDate = new Date();
            result =  currentDate.after(parseDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void process(String key, Delivery value) {
        deliveryStore.put(value.uuid, value);
        this.context.commit();
    }
}
