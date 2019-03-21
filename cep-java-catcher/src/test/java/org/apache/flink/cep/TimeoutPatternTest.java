package org.apache.flink.cep;

import org.apache.flink.cep.entity.Page;
import org.apache.flink.cep.event.EventWrapper;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TimeoutPatternTest {

    private final long MINUTES = 60 * 1000L;

    @Test
    public void testTimeoutPattern() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        long currentTimestamp = System.currentTimeMillis();

        List<Page> pages = Arrays.asList(
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 1, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 1, currentTimestamp + MINUTES),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 2, currentTimestamp + MINUTES),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 1, currentTimestamp + 3 * MINUTES),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 2, currentTimestamp + 8 * MINUTES),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 10000, Long.MAX_VALUE)
        );

        Supplier<List<EventWrapper<Page>>> supplier = ArrayList::new;
        List<EventWrapper<Page>> events = pages.stream()
                .map(p -> new EventWrapper<>(p, p.getTimestamp(), p.getUser(), "pattern1"))
                .collect(Collectors.toCollection(supplier));

        DataStream<EventWrapper<Page>> input = env.fromCollection(events)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<EventWrapper<Page>>() {
                    private long lastTimestamp;
                    @Override
                    public long extractTimestamp(EventWrapper<Page> element, long previousElementTimestamp) {
                        lastTimestamp = element.getTimestamp();
                        return element.getTimestamp();
                    }

                    @Override
                    public Watermark checkAndGetNextWatermark(EventWrapper<Page> lastElement, long extractedTimestamp) {
                        return new Watermark(lastTimestamp - 1);
                    }
                })
                .keyBy((KeySelector<EventWrapper<Page>, String>) EventWrapper::getPattern);

        List<Pattern> patterns = buildPatterns();

        List<String> resultList = new ArrayList<>();
        DataStream<String> result = CEP.pattern(input, patterns)
                .select((PatternSelectFunction<String>)
                        matchingUser -> matchingUser.f0 + "," + matchingUser.f1.toString());

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        assertEquals(Arrays.asList("pattern1,1"), resultList);
    }

    private List<Pattern> buildPatterns() {
        List<Pattern> patterns = new ArrayList<>();
        Pattern pattern1 = Pattern.begin("register").where(new IterativeCondition() {
            @Override
            public boolean filter(EventWrapper value, Context ctx) throws Exception {
                Page page = (Page)value.getEvent();
                return page.getUrl().equals("/register");
            }
        }).followedBy("login").where(new IterativeCondition() {
            @Override
            public boolean filter(EventWrapper value, Context ctx) throws Exception {
                Page page = (Page)value.getEvent();
                return page.getUrl().equals("/login");
            }
        }).followedBy("purchase").where(new IterativeCondition() {
            @Override
            public boolean filter(EventWrapper value, Context ctx) throws Exception {
                Page page = (Page)value.getEvent();
                return page.getUrl().equals("/purchase");
            }
        });
        pattern1.setId("pattern1");
        pattern1.setTimeoutMinutes(5);

        patterns.add(pattern1);
        return patterns;
    }

}
