package org.apache.flink.cep.model;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.entity.Page;
import org.apache.flink.cep.event.EventWrapper;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class NFAAbandonDataTest {


    private String simplePattern = "simplePattern";
    private String optionPattern = "optionPattern";

    @Test
    public void testSimplePattern() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        long currentTimestamp = System.currentTimeMillis();

        List<Page> pages = Arrays.asList(
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 1, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 3, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/register", 1, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/purchase", 2, currentTimestamp),
                new Page("0a1b4118dd954ec3bcc69da5138bdb96", "/login", 3, currentTimestamp)
        );

        Supplier<List<EventWrapper<Page>>> supplier = ArrayList::new;
        List<EventWrapper<Page>> events = pages.stream()
                .map(p -> new EventWrapper<>(p, p.getTimestamp(), p.getUser(), simplePattern))
                .collect(Collectors.toCollection(supplier));

        DataStream<EventWrapper<Page>> input = env.fromCollection(events).keyBy(
                (KeySelector<EventWrapper<Page>, String>) EventWrapper::getPattern);

        List<Pattern> patterns = buildPatterns();

        OutputTag<EventWrapper<Page>> outputTag = new OutputTag("xxx", TypeInformation.of(EventWrapper.class));

        List<String> resultList = new ArrayList<>();
        DataStream<String> result = CEP.pattern(input, patterns).nfaAbandonData(outputTag)
                .select((PatternSelectFunction<String>)
                        matchingUser -> matchingUser.f0 + "," + matchingUser.f1.toString());
        result.print();

        DataStream<String> nfaAbandonData =
                ((SingleOutputStreamOperator<String>) result).getSideOutput(outputTag).map(x -> x.getEvent().getUrl());

        DataStreamUtils.collect(nfaAbandonData).forEachRemaining(resultList::add);

        assertEquals(Arrays.asList("/login","/register","/login"), resultList);
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



}
