package com.github.shawven.calf.track.register;

public interface Emitter<T> {

    /**
     * Signal a normal value.
     * @param value the value to signal, not {@code null}
     */
    void onNext(T value);

    /**
     * Signal a {@link Throwable} exception.
     * @param error the {@code Throwable} to signal, not {@code null}
     */
    void onError(Throwable error);

    /**
     * Signal a completion.
     */
    void onComplete();
}
