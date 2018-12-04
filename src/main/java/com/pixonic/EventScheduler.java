package com.pixonic;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public interface EventScheduler<T> {
    Future<T> scheduleEvent(final LocalDateTime dateTime, final Callable<T> callable);

    void stopScheduler();
}