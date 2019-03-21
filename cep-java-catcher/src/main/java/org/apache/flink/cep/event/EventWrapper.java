package org.apache.flink.cep.event;

public class EventWrapper<T> implements CatcherEvent {

    private T event;

    private long timestamp;

    private int user;

    private String pattern;

    public EventWrapper(T event, long timestamp, int user, String pattern) {
        this.event = event;
        this.timestamp = timestamp;
        this.user = user;
        this.pattern = pattern;
    }

    public T getEvent() {
        return event;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getUser() {
        return this.user;
    }

    public String getPattern() {
        return this.pattern;
    }

    public void setEvent(T event) {
        this.event = event;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public void close() throws Exception {
        // TODO
    }

    @Override
    public String toString() {
        return super.toString();
    }
}