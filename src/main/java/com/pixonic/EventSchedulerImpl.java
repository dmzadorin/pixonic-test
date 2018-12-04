package com.pixonic;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class EventSchedulerImpl<T> implements EventScheduler<T> {
    private final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
    private final DelayQueue<DelayedEvent<T>> eventQueue;
    private final BlockingDeque<Event<T>> elapsedEventQueue;
    private final Thread eventProcessorThread;
    private final Thread elapsedEventProcessorThread;

    public EventSchedulerImpl() {
        eventQueue = new DelayQueue<>();
        elapsedEventQueue = new LinkedBlockingDeque<>();
        eventProcessorThread = new Thread(() -> processEvents(eventQueue));
        elapsedEventProcessorThread = new Thread(() -> processEvents(elapsedEventQueue));
        eventProcessorThread.start();
        elapsedEventProcessorThread.start();
    }

    @Override
    public Future<T> scheduleEvent(final LocalDateTime dateTime, final Callable<T> callable) {
        validate(dateTime, "Date time is required!");
        validate(callable, "Callable is required!");
        System.out.println("Got new event with start time: " + DATE_TIME_FORMATTER.format(dateTime));
        final LocalDateTime now = LocalDateTime.now();
        final CompletableFuture<T> future = new CompletableFuture<>();
        if (now.isAfter(dateTime)) {
            System.out.println("Event time is after current time, will run callable in place");
            elapsedEventQueue.add(new Event<>(dateTime, callable, future));
        } else {
            eventQueue.add(new DelayedEvent<>(dateTime, callable, future));
        }
        return future;
    }

    @Override
    public void stopScheduler() {
        eventProcessorThread.interrupt();
        elapsedEventProcessorThread.interrupt();
        try {
            eventProcessorThread.join(100);
            elapsedEventProcessorThread.join(100);
        } catch (final InterruptedException e) {
            System.out.println("Interrupted while awaiting executor threads termination");
        }
    }

    private void processEvents(final BlockingQueue<? extends Event<T>> queue) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Event<T> event = queue.take();
                final LocalDateTime dateTime = event.getDateTime();
                if (event instanceof DelayedEvent) {
                    final long creationTime = ((DelayedEvent<T>) event).creationTime;
                    System.out.println("Running callable for datetime: " + DATE_TIME_FORMATTER.format(dateTime) + ", create time: " + creationTime);
                } else {
                    System.out.println("Running callable for datetime: " + DATE_TIME_FORMATTER.format(dateTime));
                }
                executeCallable(event.callable, event.future);
            } catch (final InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void executeCallable(final Callable<T> callable, final CompletableFuture<T> future) {
        try {
            final T value = callable.call();
            future.complete(value);
        } catch (final Exception ex) {
            System.out.println("Failed to execute callable!");
            future.completeExceptionally(ex);
        }

    }

    private static void validate(final Object object, final String errorMessage) {
        if (object == null) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    static class Event<T> {
        private final LocalDateTime dateTime;
        private final Callable<T> callable;
        private final CompletableFuture<T> future;

        Event(final LocalDateTime dateTime, final Callable<T> callable, final CompletableFuture<T> future) {
            this.dateTime = dateTime;
            this.callable = callable;
            this.future = future;
        }

        public LocalDateTime getDateTime() {
            return dateTime;
        }

        public Callable<T> getCallable() {
            return callable;
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }
    }

    static class DelayedEvent<T> extends Event<T> implements Delayed {
        private final long creationTime;

        DelayedEvent(final LocalDateTime dateTime, final Callable<T> callable, final CompletableFuture<T> future) {
            super(dateTime, callable, future);
            this.creationTime = System.currentTimeMillis();
        }

        @Override
        public long getDelay(final TimeUnit unit) {
            return LocalDateTime.now().until(getDateTime(), unit.toChronoUnit());
        }

        @Override
        public int compareTo(final Delayed o) {
            if (o instanceof DelayedEvent) {
                final DelayedEvent otherEvent = (DelayedEvent) o;
                if (this.getDateTime().equals(otherEvent.getDateTime())) {
                    //If time is equal, then need to compare event creation time
                    return Long.compare(this.creationTime, otherEvent.creationTime);
                } else {
                    return this.getDateTime().compareTo(otherEvent.getDateTime());
                }
            } else {
                final long thisDelay = getDelay(TimeUnit.NANOSECONDS);
                final long otherDelay = o.getDelay(TimeUnit.NANOSECONDS);
                return Long.compare(thisDelay, otherDelay);
            }
        }
    }
}
