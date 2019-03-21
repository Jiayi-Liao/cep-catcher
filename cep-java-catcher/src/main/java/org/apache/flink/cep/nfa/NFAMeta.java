package org.apache.flink.cep.nfa;

import java.io.Serializable;
import java.util.Collection;

public class NFAMeta implements Serializable {

    private Collection<State> states;

    private long patternTimeouts = 0;

    private boolean notFollowType = false;

    public NFAMeta(Collection<State> states, long patternTimeouts) {
        this(states, patternTimeouts, false);
    }

    public NFAMeta(Collection<State> states, long patternTimeouts, boolean notFollowType) {
        this.states = states;
        this.patternTimeouts = patternTimeouts;
        this.notFollowType = notFollowType;
    }

    public Collection<State> getStates() {
        return states;
    }

    public long getPatternTimeouts() {
        return patternTimeouts;
    }

    public boolean isNotFollowType() {
        return notFollowType;
    }
}
