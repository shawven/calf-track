package com.github.shawven.calf.track.register.election;

public interface LeaderLatchListener {

    void isLeader();

    void notLeader();
}
