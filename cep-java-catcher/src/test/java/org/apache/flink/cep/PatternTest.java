package org.apache.flink.cep;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.entity.Page;
import org.apache.flink.cep.event.CatcherEvent;
import org.apache.flink.cep.event.EventWrapper;
import org.apache.flink.cep.event.PatternWrapper;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class PatternTest {

    private String simplePattern = "simplePattern";

    @Test
    public void testAppendPattern() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        long currentTimestamp = System.currentTimeMillis();

        List<Page> pages = Arrays.asList(
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/feedback", 1, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 3, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/exit", 1, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 3, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 2, currentTimestamp)
        );

        Supplier<List<CatcherEvent>> supplier = ArrayList::new;
        List<CatcherEvent> events = pages.stream()
                .map(p -> new EventWrapper<>(p, p.getTimestamp(), p.getUser(), simplePattern))
                .collect(Collectors.toCollection(supplier));

        List<CatcherEvent> patterns = new ArrayList<>();
        patterns.add(new PatternWrapper(simplePattern, buildPatterns().get(0)));

        DataStream<CatcherEvent> dataInput = env.fromCollection(events).setParallelism(1);
        DataStream<CatcherEvent> patternInput = env.fromCollection(patterns);

        DataStream<CatcherEvent> input = dataInput.connect(patternInput).keyBy((KeySelector<CatcherEvent, String>) value -> ((EventWrapper) value).getPattern(),
                (KeySelector<CatcherEvent, String>) value -> ((PatternWrapper) value).getId())
        .flatMap(new CoFlatMapFunction<CatcherEvent, CatcherEvent, CatcherEvent>() {

            List<CatcherEvent> bufferElements = new ArrayList<>();

            boolean beforePattern = true;

            @Override
            public void flatMap1(CatcherEvent value, Collector<CatcherEvent> out) throws Exception {
                if (beforePattern) {
                    bufferElements.add(value);
                } else {
                    out.collect(value);
                }
            }

            @Override
            public void flatMap2(CatcherEvent value, Collector<CatcherEvent> out) throws Exception {
                out.collect(value);
                beforePattern = false;
                bufferElements.forEach(out::collect);
            }
        });

        List<String> resultList = new ArrayList<>();
        DataStream<String> result = CEP.pattern(input, Collections.emptyList())
                .select((PatternSelectFunction<String>)
                        matchingUser -> matchingUser.f0 + "," + matchingUser.f1.toString());

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        assertEquals(Collections.singletonList(simplePattern + ",2"), resultList);
    }

    @Test
    public void testUpdatePattern() throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        long currentTimestamp = System.currentTimeMillis();

        List<Page> pages = Arrays.asList(
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/feedback", 1, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 3, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/exit", 1, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 3, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/quit", 3, currentTimestamp)
        );

        Supplier<List<CatcherEvent>> supplier = ArrayList::new;
        List<CatcherEvent> events = pages.stream()
                .map(p -> new EventWrapper<>(p, p.getTimestamp(), p.getUser(), simplePattern))
                .collect(Collectors.toCollection(supplier));

        List<CatcherEvent> patterns = new ArrayList<>();
        patterns.add(new PatternWrapper(simplePattern, updatePattern()));

        DataStream<CatcherEvent> dataInput = env.fromCollection(events).setParallelism(1);
        DataStream<CatcherEvent> patternInput = env.fromCollection(patterns);

        DataStream<CatcherEvent> input = dataInput.connect(patternInput).keyBy((KeySelector<CatcherEvent, String>) value -> ((EventWrapper) value).getPattern(),
                (KeySelector<CatcherEvent, String>) value -> ((PatternWrapper) value).getId())
                .flatMap(new CoFlatMapFunction<CatcherEvent, CatcherEvent, CatcherEvent>() {

                    List<CatcherEvent> bufferElements = new ArrayList<>();

                    boolean beforePattern = true;

                    @Override
                    public void flatMap1(CatcherEvent value, Collector<CatcherEvent> out) throws Exception {
                        if (beforePattern) {
                            bufferElements.add(value);
                        } else {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void flatMap2(CatcherEvent value, Collector<CatcherEvent> out) throws Exception {
                        out.collect(value);
                        beforePattern = false;
                        bufferElements.forEach(out::collect);
                    }
                });

        List<String> resultList = new ArrayList<>();
        DataStream<String> result = CEP.pattern(input, buildPatterns())
                .select((PatternSelectFunction<String>)
                        matchingUser -> matchingUser.f0 + "," + matchingUser.f1.toString());

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);

        assertEquals(Collections.singletonList(simplePattern + ",3"), resultList);
    }
    
    private List<Pattern> buildPatterns() {
        List<Pattern> patterns = new ArrayList<>();

        patterns.add(simplePattern());
        return patterns;
    }

    private Pattern simplePattern() {
        Pattern pattern = Pattern.begin("register").where(new IterativeCondition() {
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
        pattern.setId(simplePattern);
        return pattern;
    }

    private Pattern updatePattern() {
        Pattern pattern = Pattern.begin("register").where(new IterativeCondition() {
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
                return page.getUrl().equals("/quit");
            }
        });
        pattern.setId(simplePattern);
        return pattern;
    }
}
