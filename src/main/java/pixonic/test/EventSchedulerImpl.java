package pixonic.test;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.IntStream;

public class EventSchedulerImpl<T> implements EventScheduler<T> {
    private final PriorityBlockingQueue<Event> queue;
    private final ExecutorService executorService;

    public EventSchedulerImpl(final int threads) {
        queue = new PriorityBlockingQueue<>();
        executorService = Executors.newFixedThreadPool(threads);
        IntStream.range(0, threads).forEach(value -> executorService.submit(this::processEvents));
    }

    @Override
    public void scheduleEvent(final LocalDateTime dateTime, final Callable<T> callable) {
        validate(dateTime, "Date time is required!");
        validate(callable, "Callable is required!");
        System.out.println("Got new event with start time: " + dateTime);
        queue.add(new Event(dateTime, callable));
    }

    @Override
    public void stopScheduler() {
        executorService.shutdownNow();
    }

    private void processEvents() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Event event = queue.take();
                System.out.println("Running callable for datetime: " + event.dateTime);
                runCallable(event.callable);
                System.out.println("Finished running callable for datetime: " + event.dateTime);
            } catch (final InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runCallable(final Callable<T> callable) {
        try {
            T result = callable.call();
            System.out.println("Got result from callable: " + result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void validate(final Object object, final String errorMessage) {
        if (object == null) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    class Event implements Comparable<Event> {
        final LocalDateTime dateTime;
        final Callable<T> callable;
        final long eventTime;

        Event(final LocalDateTime dateTime, final Callable<T> callable) {
            this.dateTime = dateTime;
            this.callable = callable;
            this.eventTime = System.currentTimeMillis();
        }

        @Override
        public int compareTo(final Event o) {
            if (this.dateTime.equals(o.dateTime)) {
                return Long.compare(this.eventTime, o.eventTime);
            } else {
                return dateTime.compareTo(o.dateTime);
            }
        }
    }
}
