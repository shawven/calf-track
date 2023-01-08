package com.github.shawven.calf.track.register;

import com.github.shawven.calf.track.register.election.Election;
import com.github.shawven.calf.track.register.election.ElectionListener;

/**
 * @author xw
 * @date 2022/11/30
 */
public interface ElectionFactory {

    Election getElection(String path, String namespace, String name, long ttl, ElectionListener listener);
}
