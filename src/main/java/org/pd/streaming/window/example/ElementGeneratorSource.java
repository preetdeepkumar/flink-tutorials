package org.pd.streaming.window.example;

import org.apache.flink.shaded.netty4.io.netty.util.internal.ThreadLocalRandom;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

class ElementGeneratorSource implements SourceFunction<Element> {
    volatile boolean isRunning = true;
    final Logger logger = LoggerFactory.getLogger(ElementGeneratorSource.class);

    @Override
    public void run(SourceContext<Element> ctx) throws Exception {
        int counter = 1;
        // 20 seconds behind flink program's start time
        long eventStartTime = System.currentTimeMillis() - 20000;
        // create first event using above timestamp
        Element element = new Element(counter++, eventStartTime);
        while (isRunning) {
            logger.info("Produced Element with value {} and timestamp {}", element.getValue(), printTime(element.getTimestamp()));
            ctx.collect(element);
            // create elements and assign timestamp with randomness so that they are not same as current system clock time
            element = new Element(counter++, element.getTimestamp() + ThreadLocalRandom.current().nextLong(1000, 6000));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    // helper function to print epoch time in readable format
    String printTime(long longValue) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(longValue), ZoneId.systemDefault()).toString();
    }
}