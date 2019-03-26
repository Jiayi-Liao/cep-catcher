package org.apache.flink.cep.state;

import org.apache.flink.cep.entity.Page;
import org.apache.flink.cep.event.EventWrapper;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PageSourceFunction extends RichSourceFunction<EventWrapper<Page>> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String pattern;

    public PageSourceFunction(String pattern) {
        this.pattern = pattern;
    }

    private long currentTimestamp = System.currentTimeMillis();
    private List<Page> pages = Arrays.asList(
            new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 2, currentTimestamp),
            new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/feedback", 1, currentTimestamp),
            new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 3, currentTimestamp));
    private AtomicInteger count = new AtomicInteger(0);

    @Override
    public void run(SourceContext<EventWrapper<Page>> ctx) throws Exception {
        while (true) {
            if (count.get() > 2) {
            Thread.sleep(1000);
                count.set(0);
            } else {
                Page p = pages.get(count.get());
                EventWrapper<Page> eventWrapper = new EventWrapper<>(p, p.getTimestamp(), p.getUser(), pattern);
                ctx.collect(eventWrapper);
                count.incrementAndGet();
            }
        }
    }

    @Override
    public void cancel() {
        logger.info("Source function cancel.");
    }
}
