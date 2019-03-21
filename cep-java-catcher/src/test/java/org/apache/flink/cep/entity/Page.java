package org.apache.flink.cep.entity;

import java.io.Serializable;

public class Page implements Serializable {

    private String ai;

    private String url;

    private int user;

    private long timestamp;

    public Page(String ai, String url, int user, long timestamp) {
        this.ai = ai;
        this.url = url;
        this.user = user;
        this.timestamp = timestamp;
    }

    public String getAi() {
        return ai;
    }

    public String getUrl() {
        return url;
    }

    public int getUser() {
        return user;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
