package com.github.shawven.calf.track.register.election;

/**
 * 任务监听器
 *
 * @author xw
 * @date 2022-12-08
 */
public interface ElectionListener extends LeaderLatchListener, LeaderSelectorListener {

    @Override
    default void isLeader() {

    }

    @Override
    default void notLeader() {

    }

    @Override
    default void takeLeadership() {

    }
}
