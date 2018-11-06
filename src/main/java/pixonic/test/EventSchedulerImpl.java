package pixonic.test;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EventSchedulerImpl<T> implements EventScheduler<T> {
    private final DelayQueue<DelayedEvent<T>> eventQueue;
    private final ExecutorService executorService;

    public EventSchedulerImpl() {
        eventQueue = new DelayQueue<>();
        executorService = Executors.newFixedThreadPool(2);
        executorService.submit(this::processEvents);
    }

    @Override
    public void scheduleEvent(final LocalDateTime dateTime, final Callable<T> callable) {
        validate(dateTime, "Date time is required!");
        validate(callable, "Callable is required!");
        System.out.println("Got new event with start time: " + dateTime);
        final LocalDateTime now = LocalDateTime.now();
        if (now.isAfter(dateTime)) {
            System.out.println("Event time is after current time, will run callable in place");
            executorService.submit(callable);
        } else {
            eventQueue.add(new DelayedEvent<>(dateTime, callable));
        }
    }

    @Override
    public void stopScheduler() {
        executorService.shutdownNow();
    }

    private void processEvents() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                DelayedEvent<T> event = eventQueue.take();
                System.out.println("Running callable for datetime: " + event.dateTime + ", create time: " + event.creationTime);
                executorService.submit(event.callable);
            } catch (final InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void validate(final Object object, final String errorMessage) {
        if (object == null) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    static class DelayedEvent<T> implements Delayed {

        private final LocalDateTime dateTime;
        private final Callable<T> callable;
        private final long creationTime;

        DelayedEvent(final LocalDateTime dateTime, final Callable<T> callable) {
            this.dateTime = dateTime;
            this.callable = callable;
            this.creationTime = System.currentTimeMillis();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return LocalDateTime.now().until(dateTime, unit.toChronoUnit());
        }

        @Override
        public int compareTo(Delayed o) {
            if (o instanceof DelayedEvent) {
                final DelayedEvent otherEvent = (DelayedEvent) o;
                if (this.dateTime.equals(otherEvent.dateTime)) {
                    //If time is equal, then need to compare event creation time
                    return Long.compare(this.creationTime, otherEvent.creationTime);
                } else {
                    return dateTime.compareTo(otherEvent.dateTime);
                }
            } else {
                final long thisDelay = getDelay(TimeUnit.NANOSECONDS);
                final long otherDelay = o.getDelay(TimeUnit.NANOSECONDS);
                return Long.compare(thisDelay, otherDelay);
            }
        }
    }
}
