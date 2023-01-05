package com.github.shawven.calf.track.server.support;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 参考org.springframework.data.util.Pair
 * @see org.springframework.data.util.Pair
 *
 * @param <S>
 * @param <T>
 */
public final class Pair<S, T> {

    private final S first;
    private final T second;

    private Pair(S first, T second) {
        this.first = first;
        this.second = second;
    }


    /**
     * Creates a new Pair for the given elements.
     *
     * @param first must not be {@literal null}.
     * @param second must not be {@literal null}.
     * @return
     */
    public static <S, T> Pair<S, T> of(S first, T second) {
        return new Pair<S, T>(first, second);
    }

    /**
     * Returns the first element of the Pair.
     *
     * @return
     */
    public S getFirst() {
        return first;
    }

    /**
     * Returns the second element of the Pair.
     *
     * @return
     */
    public T getSecond() {
        return second;
    }

    /**
     * A collector to create a {@link Map} from a {@link Stream} of Pairs.
     *
     * @return
     */
    public static <S, T> Collector<Pair<S, T>, ?, Map<S, T>> toMap() {
        return Collectors.toMap(Pair::getFirst, Pair::getSecond, (o, n) -> n, LinkedHashMap::new);
    }

    /**
     * A collector to create a {@link LinkedHashMap} from a {@link Stream} of Pairs.
     *
     * @return
     */
    public static <S, T> Collector<Pair<S, T>, ?, Map<S, T>> toLinkedHashMap() {
        return Collectors.toMap(Pair::getFirst, Pair::getSecond, (o, n) -> n, LinkedHashMap::new);
    }

    /**
     * A collector to create a {@link TreeMap} from a {@link Stream} of Pairs.
     *
     * @return
     */
    public static <S, T> Collector<Pair<S, T>, ?, TreeMap<S, T>> toTreeMap() {
        return Collectors.toMap(Pair::getFirst, Pair::getSecond, (o, n) -> n, TreeMap::new);
    }
}
