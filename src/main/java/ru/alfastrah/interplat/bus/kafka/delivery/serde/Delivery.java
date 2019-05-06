package ru.alfastrah.interplat.bus.kafka.delivery.serde;

public class Delivery {
    public String uuid;
    public String queue;
    public String context;
    public String time;

    public Delivery(String uuid, String queue, String context, String time) {
        this.uuid = uuid;
        this.queue = queue;
        this.context = context;
        this.time = time;
    }
}
