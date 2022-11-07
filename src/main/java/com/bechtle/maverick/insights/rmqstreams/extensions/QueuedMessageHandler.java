package com.bechtle.maverick.insights.rmqstreams.extensions;

import com.rabbitmq.stream.Message;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueuedMessageHandler<OUT> implements com.rabbitmq.stream.MessageHandler {
    private SourceFunction.SourceContext<OUT> sourceContext;
    private final BlockingQueue<Message> queue;

    public QueuedMessageHandler() {
        this(Integer.MAX_VALUE);
    }
    public QueuedMessageHandler(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    @Override
    public void handle(Context context, Message message) {
        try {
            queue.put(message);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Message next(SourceFunction.SourceContext<OUT> sourceContext) throws InterruptedException {
        return this.queue.poll(500, TimeUnit.MILLISECONDS);
    }
}
