package com.pixonic;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;

public interface EventScheduler<T> {
    void scheduleEvent(final LocalDateTime dateTime, final Callable<T> callable);

    void stopScheduler();
}