package org.apache.flink.cep.event;

import org.apache.flink.cep.pattern.Pattern;

import java.util.ArrayList;
import java.util.List;

public class PatternWrapper implements CatcherEvent {

    private String id;

    private Pattern pattern;

    private List<String> rules = new ArrayList<>();

    public PatternWrapper(String id, Pattern pattern) {
        this.id = id;
        this.pattern = pattern;
    }

    public void setRules(List<String> rules) {
        this.rules = rules;
    }

    public List<String> getRules() {
        return this.rules;
    }

    public String getId() {
        return id;
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public String toString() {
        return id;
    }
}
